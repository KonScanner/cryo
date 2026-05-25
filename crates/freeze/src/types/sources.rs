use std::sync::Arc;

use alloy::{
    eips::BlockNumberOrTag,
    primitives::{Address, BlockNumber, Bytes, TxHash, B256, U256},
    providers::{
        ext::{DebugApi, TraceApi},
        Provider, ProviderBuilder, RootProvider,
    },
    rpc::types::{
        trace::{
            common::TraceResult,
            geth::{
                AccountState, CallConfig, CallFrame, DefaultFrame, DiffMode,
                GethDebugBuiltInTracerType, GethDebugTracerType, GethDebugTracingOptions,
                GethTrace, PreStateConfig, PreStateFrame,
            },
            parity::{
                LocalizedTransactionTrace, TraceResults, TraceResultsWithTransactionHash, TraceType,
            },
        },
        Block, BlockTransactions, BlockTransactionsKind, Filter, Log, Transaction,
        TransactionInput, TransactionReceipt, TransactionRequest,
    },
    transports::{http::reqwest::Url, RpcError, TransportErrorKind},
};
use governor::{
    clock::DefaultClock,
    middleware::NoOpMiddleware,
    state::{direct::NotKeyed, InMemoryState},
    Quota,
};
use std::num::NonZeroU32;
use tokio::sync::{AcquireError, Semaphore, SemaphorePermit};

use crate::CollectError;

/// RateLimiter based on governor crate
pub type RateLimiter = governor::RateLimiter<NotKeyed, InMemoryState, DefaultClock, NoOpMiddleware>;

/// Options for fetching data from node
#[derive(Clone, Debug)]
pub struct Source {
    /// provider for the primary (L2 or L1) RPC
    pub provider: RootProvider,
    /// chain_id of primary network
    pub chain_id: u64,
    /// number of blocks per log request
    pub inner_request_size: u64,
    /// Maximum chunks collected concurrently
    pub max_concurrent_chunks: Option<u64>,
    /// Rpc Url
    pub rpc_url: String,
    /// semaphore for controlling concurrency
    pub semaphore: Arc<Option<Semaphore>>,
    /// rate limiter for controlling request rate
    pub rate_limiter: Arc<Option<RateLimiter>>,
    /// Labels (these are non-functional)
    pub labels: SourceLabels,
    /// Optional secondary provider for the L1 (settlement) chain.
    ///
    /// Populated when `--l1-rpc <url>` is passed; consumed by L2-specific
    /// datasets that need to read L1-side events (batch postings, deposits,
    /// output proposals, etc.). `None` for single-chain runs. The same
    /// `semaphore` and `rate_limiter` gate calls on both providers — this is
    /// intentionally simple for now; a future change may give the L1 path
    /// its own bucket.
    pub l1_provider: Option<RootProvider>,
    /// chain_id reported by the L1 RPC. `None` if `l1_provider` is `None`.
    pub l1_chain_id: Option<u64>,
    /// rpc url passed via `--l1-rpc`. `None` if not configured.
    pub l1_rpc_url: Option<String>,
}

impl Source {
    /// Returns all receipts for a block.
    /// Tries to use `eth_getBlockReceipts` first, and falls back to `eth_getTransactionReceipt`
    pub async fn get_tx_receipts_in_block(
        &self,
        block: &Block<Transaction>,
    ) -> Result<Vec<TransactionReceipt>> {
        let block_number = block.header.number;
        if let Ok(Some(receipts)) = self.get_block_receipts(block_number).await {
            return Ok(receipts);
        }

        self.get_tx_receipts(block.transactions.clone()).await
    }

    /// Returns all receipts for vector of transactions.
    ///
    /// Issues a single JSON-RPC batch with one `eth_getTransactionReceipt`
    /// per transaction hash (see [`Self::get_transaction_receipts_batch`])
    /// — one HTTP round-trip instead of N. Errors if any receipt is
    /// missing (the per-call path used to error there too).
    pub async fn get_tx_receipts(
        &self,
        transactions: BlockTransactions<Transaction>,
    ) -> Result<Vec<TransactionReceipt>> {
        let hashes: Vec<TxHash> = transactions
            .as_transactions()
            .map(|txs| txs.iter().map(|tx| *tx.inner.tx_hash()).collect())
            .unwrap_or_default();
        let receipts = self.get_transaction_receipts_batch(hashes).await?;
        receipts
            .into_iter()
            .map(|r| {
                r.ok_or_else(|| CollectError::CollectError("could not find tx receipt".to_string()))
            })
            .collect()
    }

    /// Fetch transaction receipts for many hashes in a single JSON-RPC batch.
    ///
    /// Sends one HTTP request carrying N embedded `eth_getTransactionReceipt`
    /// calls and returns the parallel list of receipts. Compared to the
    /// per-hash fan-out used previously (N `tokio::spawn`'d tasks, each a
    /// separate HTTP call), this is one round-trip instead of N — the
    /// structural win is largest when N is big (e.g. block.transactions.len()
    /// on a busy mainnet block, often > 100).
    ///
    /// Acquires a single semaphore permit for the whole batch; callers
    /// wanting stricter throttling should chunk the input and call this
    /// once per chunk.
    ///
    /// # Errors
    /// - Transport failure on the batch envelope itself.
    /// - Individual missing receipts surface as `None` in the returned vector, not as errors —
    ///   callers can map `None` to the semantics they want.
    pub async fn get_transaction_receipts_batch(
        &self,
        hashes: Vec<TxHash>,
    ) -> Result<Vec<Option<TransactionReceipt>>> {
        if hashes.is_empty() {
            return Ok(Vec::new())
        }
        let _permit = self.permit_request().await;
        let mut batch = alloy::rpc::client::BatchRequest::new(self.provider.client());
        let mut waiters: Vec<alloy::rpc::client::Waiter<Option<TransactionReceipt>>> =
            Vec::with_capacity(hashes.len());
        for h in &hashes {
            let w = batch
                .add_call("eth_getTransactionReceipt", &(*h,))
                .map_err(|e| CollectError::CollectError(format!("batch add_call failed: {e:?}")))?;
            waiters.push(w);
        }
        batch.await.map_err(|e| CollectError::CollectError(format!("batch send failed: {e:?}")))?;
        let mut out = Vec::with_capacity(waiters.len());
        for w in waiters {
            let r = w
                .await
                .map_err(|e| CollectError::CollectError(format!("batch waiter failed: {e:?}")))?;
            out.push(r);
        }
        Ok(out)
    }

    /// Fetch many transactions by hash in a single JSON-RPC batch.
    ///
    /// One HTTP round-trip carrying N embedded `eth_getTransactionByHash`
    /// calls. Missing transactions surface as `None` in the returned vector.
    /// See [`Self::get_transaction_receipts_batch`] for the design rationale.
    ///
    /// # Errors
    /// Transport failure on the batch envelope.
    pub async fn get_transactions_by_hash_batch(
        &self,
        hashes: Vec<TxHash>,
    ) -> Result<Vec<Option<Transaction>>> {
        if hashes.is_empty() {
            return Ok(Vec::new())
        }
        let _permit = self.permit_request().await;
        let mut batch = alloy::rpc::client::BatchRequest::new(self.provider.client());
        let mut waiters: Vec<alloy::rpc::client::Waiter<Option<Transaction>>> =
            Vec::with_capacity(hashes.len());
        for h in &hashes {
            let w = batch
                .add_call("eth_getTransactionByHash", &(*h,))
                .map_err(|e| CollectError::CollectError(format!("batch add_call failed: {e:?}")))?;
            waiters.push(w);
        }
        batch.await.map_err(|e| CollectError::CollectError(format!("batch send failed: {e:?}")))?;
        let mut out = Vec::with_capacity(waiters.len());
        for w in waiters {
            out.push(
                w.await.map_err(|e| {
                    CollectError::CollectError(format!("batch waiter failed: {e:?}"))
                })?,
            );
        }
        Ok(out)
    }

    /// Fetch many blocks (transaction hashes only) in a single JSON-RPC batch.
    ///
    /// One HTTP round-trip carrying N embedded `eth_getBlockByNumber` calls
    /// with `fullTransactions = false`. For full transactions, use
    /// [`Self::get_full_blocks_batch`].
    ///
    /// # Errors
    /// Transport failure on the batch envelope.
    pub async fn get_blocks_batch(&self, block_numbers: Vec<u64>) -> Result<Vec<Option<Block>>> {
        self.get_blocks_batch_impl(block_numbers, false).await
    }

    /// Fetch many blocks with full transaction bodies in a single JSON-RPC batch.
    ///
    /// One HTTP round-trip carrying N embedded `eth_getBlockByNumber` calls
    /// with `fullTransactions = true`. Heavier per-request than
    /// [`Self::get_blocks_batch`] — N is best kept smaller for full-tx mode.
    ///
    /// # Errors
    /// Transport failure on the batch envelope.
    pub async fn get_full_blocks_batch(
        &self,
        block_numbers: Vec<u64>,
    ) -> Result<Vec<Option<Block>>> {
        self.get_blocks_batch_impl(block_numbers, true).await
    }

    async fn get_blocks_batch_impl(
        &self,
        block_numbers: Vec<u64>,
        full_transactions: bool,
    ) -> Result<Vec<Option<Block>>> {
        if block_numbers.is_empty() {
            return Ok(Vec::new())
        }
        let _permit = self.permit_request().await;
        let mut batch = alloy::rpc::client::BatchRequest::new(self.provider.client());
        let mut waiters: Vec<alloy::rpc::client::Waiter<Option<Block>>> =
            Vec::with_capacity(block_numbers.len());
        for n in &block_numbers {
            // eth_getBlockByNumber expects a hex-encoded quantity as the first param.
            let tag = BlockNumberOrTag::Number(*n);
            let w = batch
                .add_call("eth_getBlockByNumber", &(tag, full_transactions))
                .map_err(|e| CollectError::CollectError(format!("batch add_call failed: {e:?}")))?;
            waiters.push(w);
        }
        batch.await.map_err(|e| CollectError::CollectError(format!("batch send failed: {e:?}")))?;
        let mut out = Vec::with_capacity(waiters.len());
        for w in waiters {
            out.push(
                w.await.map_err(|e| {
                    CollectError::CollectError(format!("batch waiter failed: {e:?}"))
                })?,
            );
        }
        Ok(out)
    }

    /// Fetch traces for many transactions in a single JSON-RPC batch.
    ///
    /// Uses the parity `trace_transaction` method. One HTTP round-trip
    /// carrying N embedded calls. Each entry in the returned vector is the
    /// `Vec<LocalizedTransactionTrace>` for the corresponding input hash.
    /// Failed/missing traces surface as an empty `Vec`.
    ///
    /// # Errors
    /// Transport failure on the batch envelope.
    pub async fn trace_transactions_batch(
        &self,
        hashes: Vec<TxHash>,
    ) -> Result<Vec<Vec<LocalizedTransactionTrace>>> {
        if hashes.is_empty() {
            return Ok(Vec::new())
        }
        let _permit = self.permit_request().await;
        let mut batch = alloy::rpc::client::BatchRequest::new(self.provider.client());
        let mut waiters: Vec<alloy::rpc::client::Waiter<Vec<LocalizedTransactionTrace>>> =
            Vec::with_capacity(hashes.len());
        for h in &hashes {
            let w = batch
                .add_call("trace_transaction", &(*h,))
                .map_err(|e| CollectError::CollectError(format!("batch add_call failed: {e:?}")))?;
            waiters.push(w);
        }
        batch.await.map_err(|e| CollectError::CollectError(format!("batch send failed: {e:?}")))?;
        let mut out = Vec::with_capacity(waiters.len());
        for w in waiters {
            out.push(
                w.await.map_err(|e| {
                    CollectError::CollectError(format!("batch waiter failed: {e:?}"))
                })?,
            );
        }
        Ok(out)
    }
}

const DEFAULT_INNER_REQUEST_SIZE: u64 = 100;
const DEFAULT_MAX_RETRIES: u32 = 5;
const DEFAULT_INTIAL_BACKOFF: u64 = 5;
const DEFAULT_MAX_CONCURRENT_CHUNKS: u64 = 4;
const DEFAULT_MAX_CONCURRENT_REQUESTS: u64 = 100;

/// builder
impl Source {
    /// initialize source with default concurrency limits and no per-second rate cap.
    ///
    /// See [`Source::init_with_limits`] to override `max_concurrent_requests` /
    /// `requests_per_second` from a programmatic / Python caller.
    pub async fn init(rpc_url: Option<String>) -> Result<Source> {
        Self::init_with_limits(rpc_url, None, None).await
    }

    /// initialize source with explicit concurrency + rate caps.
    ///
    /// `max_concurrent_requests`:
    /// * `None` → [`DEFAULT_MAX_CONCURRENT_REQUESTS`] (100) — matches the CLI default.
    /// * `Some(0)` → no semaphore (unlimited concurrency).
    /// * `Some(n)` for `n > 0` → semaphore with `n` permits.
    ///
    /// `requests_per_second`:
    /// * `None` or `Some(0)` → no rate limiter.
    /// * `Some(n)` for `n > 0` → `governor` direct rate limiter at `n` req/s with burst 1.
    /// * Values above `u32::MAX` are treated as "no limit" (a saturating ceiling rather than silent
    ///   truncation to a wrong-but-plausible u32 value).
    ///
    /// Both limits are honoured by [`Source`]'s internal batch helpers
    /// (`get_transaction_receipts_batch`, `get_blocks_batch`, etc.) which
    /// acquire a single permit for the whole batch.
    ///
    /// # Errors
    /// Returns [`CollectError::RPCError`] if the chain id cannot be fetched
    /// from the configured RPC.
    ///
    /// # Panics
    /// Panics on an unparseable `rpc_url` — callers should validate the URL
    /// upstream. (TODO: move to a typed `Result` here too.)
    pub async fn init_with_limits(
        rpc_url: Option<String>,
        max_concurrent_requests: Option<u64>,
        requests_per_second: Option<u64>,
    ) -> Result<Source> {
        let rpc_url: String = parse_rpc_url(rpc_url);
        let parsed_rpc_url: Url = rpc_url.parse().expect("rpc url is not valid");
        let provider: RootProvider =
            ProviderBuilder::default().connect_http(parsed_rpc_url.clone());
        let chain_id = provider
            .get_chain_id()
            .await
            .map_err(|_| CollectError::RPCError("could not get chain_id".to_string()))?;

        let max_concurrent_requests =
            max_concurrent_requests.unwrap_or(DEFAULT_MAX_CONCURRENT_REQUESTS);
        let semaphore = if max_concurrent_requests > 0 {
            // `as usize` would *wrap* on 32-bit hosts (e.g. 5_000_000_000 → ~705M)
            // — saturate instead so an overflowing request count maps to "no cap"
            // rather than a silently-wrong small one.
            let permits = usize::try_from(max_concurrent_requests).unwrap_or(usize::MAX);
            Some(Semaphore::new(permits))
        } else {
            None
        };

        // Burst of 1 keeps the cap a hard ceiling — matches the CLI path in
        // crates/cli/src/parse/source.rs. Use `const ONE` so the unwrap is
        // proven at compile time, not at call time.
        const ONE: NonZeroU32 = match NonZeroU32::new(1) {
            Some(n) => n,
            None => unreachable!(),
        };
        let rate_limiter = requests_per_second
            .filter(|&rps| rps > 0)
            .and_then(|rps| u32::try_from(rps).ok())
            .and_then(NonZeroU32::new)
            .map(|value| {
                let quota = Quota::per_second(value).allow_burst(ONE);
                RateLimiter::direct(quota)
            });

        let provider: RootProvider = ProviderBuilder::default().connect_http(parsed_rpc_url);

        let source = Source {
            provider,
            chain_id,
            inner_request_size: DEFAULT_INNER_REQUEST_SIZE,
            max_concurrent_chunks: Some(DEFAULT_MAX_CONCURRENT_CHUNKS),
            rpc_url,
            labels: SourceLabels {
                max_concurrent_requests: Some(max_concurrent_requests),
                max_requests_per_second: requests_per_second,
                max_retries: Some(DEFAULT_MAX_RETRIES),
                initial_backoff: Some(DEFAULT_INTIAL_BACKOFF),
            },
            rate_limiter: Arc::new(rate_limiter),
            semaphore: Arc::new(semaphore),
            l1_provider: None,
            l1_chain_id: None,
            l1_rpc_url: None,
        };

        Ok(source)
    }

    /// Attach an L1 (settlement) RPC to an existing source.
    ///
    /// Used by L2-specific datasets that need to read L1-side events. The
    /// resulting `Source` shares its semaphore + rate limiter with the L2
    /// path, so an aggressive L1 RPC will spend permits from the same pool.
    ///
    /// # Errors
    /// Returns [`CollectError::RPCError`] if the L1 chain id cannot be fetched.
    ///
    /// # Panics
    /// Panics on an unparseable `l1_rpc_url`.
    pub async fn with_l1_rpc(mut self, l1_rpc_url: String) -> Result<Source> {
        let parsed: Url = l1_rpc_url.parse().expect("l1 rpc url is not valid");
        let provider: RootProvider = ProviderBuilder::default().connect_http(parsed);
        let chain_id = provider
            .get_chain_id()
            .await
            .map_err(|_| CollectError::RPCError("could not get l1 chain_id".to_string()))?;
        self.l1_provider = Some(provider);
        self.l1_chain_id = Some(chain_id);
        self.l1_rpc_url = Some(l1_rpc_url);
        Ok(self)
    }

    /// Borrow the configured L1 provider, or fail with a clear message.
    ///
    /// Datasets that require L1 data should call this rather than indexing
    /// `self.l1_provider` so the error message points at the missing CLI flag.
    ///
    /// # Errors
    /// Returns [`CollectError::CollectError`] if `--l1-rpc` was not configured.
    pub fn require_l1_provider(&self) -> Result<&RootProvider> {
        self.l1_provider.as_ref().ok_or_else(|| {
            CollectError::CollectError(
                "this dataset requires an L1 RPC: pass --l1-rpc <url>".to_string(),
            )
        })
    }
}

fn parse_rpc_url(rpc_url: Option<String>) -> String {
    let mut url = match rpc_url {
        Some(url) => url.clone(),
        _ => match std::env::var("ETH_RPC_URL") {
            Ok(url) => url,
            Err(_e) => {
                println!("must provide --rpc or set ETH_RPC_URL");
                std::process::exit(0);
            }
        },
    };
    if !url.starts_with("http") {
        url = "http://".to_string() + url.as_str();
    };
    url
}

// builder

// struct SourceBuilder {
//     /// Shared provider for rpc data
//     pub fetcher: Option<Arc<Fetcher<RetryClient<Http>>>>,
//     /// chain_id of network
//     pub chain_id: Option<u64>,
//     /// number of blocks per log request
//     pub inner_request_size: Option<u64>,
//     /// Maximum chunks collected concurrently
//     pub max_concurrent_chunks: Option<u64>,
//     /// Rpc Url
//     pub rpc_url: Option<String>,
//     /// Labels (these are non-functional)
//     pub labels: Option<SourceLabels>,
// }

// impl SourceBuilder {
//     fn new(mut self) -> SourceBuilder {
//     }

//     fn build(self) -> Source {
//     }
// }

/// source labels (non-functional)
#[derive(Clone, Debug, Default)]
pub struct SourceLabels {
    /// Maximum requests collected concurrently
    pub max_concurrent_requests: Option<u64>,
    /// Maximum requests per second
    pub max_requests_per_second: Option<u64>,
    /// Max retries
    pub max_retries: Option<u32>,
    /// Initial backoff
    pub initial_backoff: Option<u64>,
}

/// Wrapper over `Provider<N>` that adds concurrency and rate limiting controls
#[derive(Debug)]
pub struct Fetcher<N: alloy::providers::Network> {
    /// provider data source
    pub provider: RootProvider<N>,
    /// semaphore for controlling concurrency
    pub semaphore: Option<Semaphore>,
    /// rate limiter for controlling request rate
    pub rate_limiter: Option<RateLimiter>,
}

type Result<T> = ::core::result::Result<T, CollectError>;

// impl<P: JsonRpcClient> Fetcher<P> {
impl Source {
    /// Returns an array (possibly empty) of logs that match the filter
    pub async fn get_logs(&self, filter: &Filter) -> Result<Vec<Log>> {
        let _permit = self.permit_request().await;
        Self::map_err(self.provider.get_logs(filter).await)
    }

    /// Replays all transactions in a block returning the requested traces for each transaction
    pub async fn trace_replay_block_transactions(
        &self,
        block: BlockNumberOrTag,
        trace_types: Vec<TraceType>,
    ) -> Result<Vec<TraceResultsWithTransactionHash>> {
        let _permit = self.permit_request().await;
        Self::map_err(
            self.provider
                .trace_replay_block_transactions(block.into())
                .trace_types(trace_types)
                .await,
        )
    }

    /// Get state diff traces of block
    pub async fn trace_block_state_diffs(
        &self,
        block: u32,
        include_transaction_hashes: bool,
    ) -> Result<(Option<u32>, Vec<Option<Vec<u8>>>, Vec<TraceResultsWithTransactionHash>)> {
        // get traces
        let result = self
            .trace_replay_block_transactions(
                BlockNumberOrTag::Number(block as u64),
                vec![TraceType::StateDiff],
            )
            .await?;

        // get transactions
        let txs = if include_transaction_hashes {
            let transactions = self
                .get_block(block as u64, BlockTransactionsKind::Hashes)
                .await?
                .ok_or(CollectError::CollectError("could not find block".to_string()))?
                .transactions;
            match transactions {
                BlockTransactions::Hashes(hashes) => {
                    hashes.into_iter().map(|tx| Some(tx.to_vec())).collect()
                }
                _ => return Err(CollectError::CollectError("wrong transaction format".to_string())),
            }
        } else {
            vec![None; result.len()]
        };

        Ok((Some(block), txs, result))
    }

    /// Get VM traces of block
    pub async fn trace_block_vm_traces(
        &self,
        block: u32,
    ) -> Result<(Option<u32>, Option<Vec<u8>>, Vec<TraceResultsWithTransactionHash>)> {
        let result = self
            .trace_replay_block_transactions(
                BlockNumberOrTag::Number(block as u64),
                vec![TraceType::VmTrace],
            )
            .await;
        Ok((Some(block), None, result?))
    }

    /// Replays a transaction, returning the traces
    pub async fn trace_replay_transaction(
        &self,
        tx_hash: TxHash,
        trace_types: Vec<TraceType>,
    ) -> Result<TraceResults> {
        let _permit = self.permit_request().await;
        Self::map_err(
            self.provider.trace_replay_transaction(tx_hash).trace_types(trace_types).await,
        )
    }

    /// Get state diff traces of transaction
    pub async fn trace_transaction_state_diffs(
        &self,
        transaction_hash: Vec<u8>,
    ) -> Result<(Option<u32>, Vec<Option<Vec<u8>>>, Vec<TraceResults>)> {
        let result = self
            .trace_replay_transaction(
                B256::from_slice(&transaction_hash),
                vec![TraceType::StateDiff],
            )
            .await;
        Ok((None, vec![Some(transaction_hash)], vec![result?]))
    }

    /// Get VM traces of transaction
    pub async fn trace_transaction_vm_traces(
        &self,
        transaction_hash: Vec<u8>,
    ) -> Result<(Option<u32>, Option<Vec<u8>>, Vec<TraceResults>)> {
        let result = self
            .trace_replay_transaction(B256::from_slice(&transaction_hash), vec![TraceType::VmTrace])
            .await;
        Ok((None, Some(transaction_hash), vec![result?]))
    }

    /// Gets the transaction with transaction_hash
    pub async fn get_transaction_by_hash(&self, tx_hash: TxHash) -> Result<Option<Transaction>> {
        let _permit = self.permit_request().await;
        Self::map_err(self.provider.get_transaction_by_hash(tx_hash).await)
    }

    /// Gets the transaction receipt with transaction_hash
    pub async fn get_transaction_receipt(
        &self,
        tx_hash: TxHash,
    ) -> Result<Option<TransactionReceipt>> {
        let _permit = self.permit_request().await;
        Self::map_err(self.provider.get_transaction_receipt(tx_hash).await)
    }

    /// Gets the block at `block_num` (transaction hashes only)
    pub async fn get_block(
        &self,
        block_num: u64,
        kind: BlockTransactionsKind,
    ) -> Result<Option<Block>> {
        let _permit = self.permit_request().await;
        Self::map_err(self.provider.get_block(block_num.into()).kind(kind).await)
    }

    /// Gets the block with `block_hash` (transaction hashes only)
    pub async fn get_block_by_hash(
        &self,
        block_hash: B256,
        kind: BlockTransactionsKind,
    ) -> Result<Option<Block>> {
        let _permit = self.permit_request().await;
        Self::map_err(self.provider.get_block(block_hash.into()).kind(kind).await)
    }

    /// Returns all receipts for a block.
    /// Note that this uses the `eth_getBlockReceipts` method which is not supported by all nodes.
    /// Consider using `FetcherExt::get_tx_receipts_in_block` which takes a block, and falls back to
    /// `eth_getTransactionReceipt` if `eth_getBlockReceipts` is not supported.
    pub async fn get_block_receipts(
        &self,
        block_num: u64,
    ) -> Result<Option<Vec<TransactionReceipt>>> {
        let _permit = self.permit_request().await;
        Self::map_err(self.provider.get_block_receipts(block_num.into()).await)
    }

    /// Returns traces created at given block
    pub async fn trace_block(
        &self,
        block_num: BlockNumber,
    ) -> Result<Vec<LocalizedTransactionTrace>> {
        let _permit = self.permit_request().await;
        Self::map_err(self.provider.trace_block(block_num.into()).await)
    }

    /// Returns all traces of a given transaction
    pub async fn trace_transaction(
        &self,
        tx_hash: TxHash,
    ) -> Result<Vec<LocalizedTransactionTrace>> {
        let _permit = self.permit_request().await;
        Self::map_err(self.provider.trace_transaction(tx_hash).await)
    }

    /// Deprecated
    pub async fn call(
        &self,
        transaction: TransactionRequest,
        block_number: BlockNumber,
    ) -> Result<Bytes> {
        let _permit = self.permit_request().await;
        Self::map_err(self.provider.call(transaction).block(block_number.into()).await)
    }

    /// Returns traces for given call data
    pub async fn trace_call(
        &self,
        transaction: TransactionRequest,
        trace_type: Vec<TraceType>,
        block_number: Option<BlockNumber>,
    ) -> Result<TraceResults> {
        let _permit = self.permit_request().await;
        if let Some(bn) = block_number {
            return Self::map_err(
                self.provider
                    .trace_call(&transaction)
                    .trace_types(trace_type)
                    .block_id(bn.into())
                    .await,
            );
        }
        Self::map_err(self.provider.trace_call(&transaction).trace_types(trace_type).await)
    }

    /// Get nonce of address
    pub async fn get_transaction_count(
        &self,
        address: Address,
        block_number: BlockNumber,
    ) -> Result<u64> {
        let _permit = self.permit_request().await;
        Self::map_err(
            self.provider.get_transaction_count(address).block_id(block_number.into()).await,
        )
    }

    /// Get code at address
    pub async fn get_balance(&self, address: Address, block_number: BlockNumber) -> Result<U256> {
        let _permit = self.permit_request().await;
        Self::map_err(self.provider.get_balance(address).block_id(block_number.into()).await)
    }

    /// Get code at address
    pub async fn get_code(&self, address: Address, block_number: BlockNumber) -> Result<Bytes> {
        let _permit = self.permit_request().await;
        Self::map_err(self.provider.get_code_at(address).block_id(block_number.into()).await)
    }

    /// Get stored data at given location
    pub async fn get_storage_at(
        &self,
        address: Address,
        slot: U256,
        block_number: BlockNumber,
    ) -> Result<U256> {
        let _permit = self.permit_request().await;
        Self::map_err(
            self.provider.get_storage_at(address, slot).block_id(block_number.into()).await,
        )
    }

    /// Get the block number
    pub async fn get_block_number(&self) -> Result<u64> {
        Self::map_err(self.provider.get_block_number().await)
    }

    // extra helpers below

    /// block number of transaction
    pub async fn get_transaction_block_number(&self, transaction_hash: Vec<u8>) -> Result<u32> {
        let block = self.get_transaction_by_hash(B256::from_slice(&transaction_hash)).await?;
        let block = block.ok_or(CollectError::CollectError("could not get block".to_string()))?;
        Ok(block
            .block_number
            .ok_or(CollectError::CollectError("could not get block number".to_string()))?
            as u32)
    }

    /// block number of transaction
    pub async fn get_transaction_logs(&self, transaction_hash: Vec<u8>) -> Result<Vec<Log>> {
        Ok(self
            .get_transaction_receipt(B256::from_slice(&transaction_hash))
            .await?
            .ok_or(CollectError::CollectError("transaction receipt not found".to_string()))?
            .inner
            .logs()
            .to_vec())
    }

    /// Return output data of a contract call
    pub async fn call2(
        &self,
        address: Address,
        call_data: Vec<u8>,
        block_number: BlockNumber,
    ) -> Result<Bytes> {
        let transaction = TransactionRequest {
            to: Some(address.into()),
            input: TransactionInput::new(call_data.into()),
            ..Default::default()
        };
        let _permit = self.permit_request().await;
        Self::map_err(self.provider.call(transaction).block(block_number.into()).await)
    }

    /// Return output data of a contract call
    pub async fn trace_call2(
        &self,
        address: Address,
        call_data: Vec<u8>,
        trace_type: Vec<TraceType>,
        block_number: Option<BlockNumber>,
    ) -> Result<TraceResults> {
        let transaction = TransactionRequest {
            to: Some(address.into()),
            input: TransactionInput::new(call_data.into()),
            ..Default::default()
        };
        let _permit = self.permit_request().await;
        if let Some(bn) = block_number {
            Self::map_err(
                self.provider
                    .trace_call(&transaction)
                    .trace_types(trace_type)
                    .block_id(bn.into())
                    .await,
            )
        } else {
            Self::map_err(self.provider.trace_call(&transaction).trace_types(trace_type).await)
        }
    }

    /// get geth debug block traces
    pub async fn geth_debug_trace_block(
        &self,
        block_number: u32,
        options: GethDebugTracingOptions,
        include_transaction_hashes: bool,
    ) -> Result<(Option<u32>, Vec<Option<Vec<u8>>>, Vec<TraceResult<GethTrace, String>>)> {
        let traces = {
            let _permit = self.permit_request().await;
            Self::map_err(
                self.provider
                    .debug_trace_block_by_number(
                        BlockNumberOrTag::Number(block_number.into()),
                        options,
                    )
                    .await,
            )?
        };

        let txs = if include_transaction_hashes {
            match self.get_block(block_number as u64, BlockTransactionsKind::Hashes).await? {
                Some(block) => block
                    .transactions
                    .as_hashes()
                    .unwrap()
                    .iter()
                    .map(|x| Some(x.to_vec()))
                    .collect(),
                None => {
                    return Err(CollectError::CollectError(
                        "could not get block for txs".to_string(),
                    ))
                }
            }
        } else {
            vec![None; traces.len()]
        };

        Ok((Some(block_number), txs, traces))
    }

    /// get geth debug block call traces
    pub async fn geth_debug_trace_block_javascript_traces(
        &self,
        js_tracer: String,
        block_number: u32,
        include_transaction_hashes: bool,
    ) -> Result<(Option<u32>, Vec<Option<Vec<u8>>>, Vec<serde_json::Value>)> {
        let tracer = GethDebugTracerType::JsTracer(js_tracer);
        let options = GethDebugTracingOptions { tracer: Some(tracer), ..Default::default() };
        let (block, txs, traces) =
            self.geth_debug_trace_block(block_number, options, include_transaction_hashes).await?;

        let mut calls = Vec::new();
        for trace in traces.into_iter() {
            match trace {
                TraceResult::Success { result, tx_hash } => match result {
                    GethTrace::JS(value) => calls.push(value),
                    _ => {
                        return Err(CollectError::CollectError(format!(
                            "invalid trace result in tx {:?}",
                            tx_hash
                        )))
                    }
                },
                TraceResult::Error { error, tx_hash } => {
                    return Err(CollectError::CollectError(format!(
                        "invalid trace result in tx {:?}: {}",
                        tx_hash, error
                    )))
                }
            }
        }
        Ok((block, txs, calls))
    }

    /// get geth debug block opcode traces
    pub async fn geth_debug_trace_block_opcodes(
        &self,
        block_number: u32,
        include_transaction_hashes: bool,
        options: GethDebugTracingOptions,
    ) -> Result<(Option<u32>, Vec<Option<Vec<u8>>>, Vec<DefaultFrame>)> {
        let (block, txs, traces) =
            self.geth_debug_trace_block(block_number, options, include_transaction_hashes).await?;

        let mut calls = Vec::new();
        for trace in traces.into_iter() {
            match trace {
                TraceResult::Success { result, tx_hash } => match result {
                    GethTrace::Default(frame) => calls.push(frame),
                    _ => {
                        return Err(CollectError::CollectError(format!(
                            "invalid trace result in tx {:?}",
                            tx_hash
                        )))
                    }
                },
                TraceResult::Error { error, tx_hash } => {
                    return Err(CollectError::CollectError(format!(
                        "inalid trace result in tx {:?}: {}",
                        tx_hash, error
                    )));
                }
            }
        }
        Ok((block, txs, calls))
    }

    /// get geth debug block 4byte traces
    pub async fn geth_debug_trace_block_4byte_traces(
        &self,
        block_number: u32,
        include_transaction_hashes: bool,
    ) -> Result<(Option<u32>, Vec<Option<Vec<u8>>>, Vec<BTreeMap<String, u64>>)> {
        let tracer = GethDebugTracerType::BuiltInTracer(GethDebugBuiltInTracerType::FourByteTracer);
        let options = GethDebugTracingOptions { tracer: Some(tracer), ..Default::default() };
        let (block, txs, traces) =
            self.geth_debug_trace_block(block_number, options, include_transaction_hashes).await?;

        let mut calls = Vec::new();
        for trace in traces.into_iter() {
            match trace {
                // GethTrace::Known(GethTraceFrame::FourByteTracer(FourByteFrame(frame))) => {
                //     calls.push(frame)
                // }
                // GethTrace::Known(GethTraceFrame::NoopTracer(_)) => {}
                // _ => return Err(CollectError::CollectError("invalid trace result".to_string())),
                TraceResult::Success { result, tx_hash } => match result {
                    GethTrace::FourByteTracer(frame) => calls.push(frame.0),
                    GethTrace::NoopTracer(_) => {}
                    _ => {
                        return Err(CollectError::CollectError(format!(
                            "invalid trace result in tx {:?}",
                            tx_hash
                        )))
                    }
                },
                TraceResult::Error { error, tx_hash } => {
                    return Err(CollectError::CollectError(format!(
                        "invalid trace result in tx {:?}: {}",
                        tx_hash, error
                    )));
                }
            }
        }
        Ok((block, txs, calls))
    }

    /// get geth debug block call traces
    pub async fn geth_debug_trace_block_prestate(
        &self,
        block_number: u32,
        include_transaction_hashes: bool,
    ) -> Result<(Option<u32>, Vec<Option<Vec<u8>>>, Vec<BTreeMap<Address, AccountState>>)> {
        let tracer = GethDebugTracerType::BuiltInTracer(GethDebugBuiltInTracerType::PreStateTracer);
        let options = GethDebugTracingOptions { tracer: Some(tracer), ..Default::default() };
        let (block, txs, traces) =
            self.geth_debug_trace_block(block_number, options, include_transaction_hashes).await?;

        let mut calls = Vec::new();
        for trace in traces.into_iter() {
            match trace {
                // GethTrace::Known(GethTraceFrame::PreStateTracer(PreStateFrame::Default(
                //     PreStateMode(frame),
                // ))) => calls.push(frame),
                // _ => return Err(CollectError::CollectError("invalid trace result".to_string())),
                TraceResult::Success { result, tx_hash } => match result {
                    GethTrace::PreStateTracer(PreStateFrame::Default(frame)) => calls.push(frame.0),
                    _ => {
                        return Err(CollectError::CollectError(format!(
                            "invalid trace result in tx {:?}",
                            tx_hash
                        )))
                    }
                },
                TraceResult::Error { error, tx_hash } => {
                    return Err(CollectError::CollectError(format!(
                        "invalid trace result in tx {:?}: {}",
                        tx_hash, error
                    )));
                }
            }
        }
        Ok((block, txs, calls))
    }

    /// get geth debug block call traces
    pub async fn geth_debug_trace_block_calls(
        &self,
        block_number: u32,
        include_transaction_hashes: bool,
    ) -> Result<(Option<u32>, Vec<Option<Vec<u8>>>, Vec<CallFrame>)> {
        let tracer = GethDebugTracerType::BuiltInTracer(GethDebugBuiltInTracerType::CallTracer);
        // let config = GethDebugTracerConfig::BuiltInTracer(
        //     GethDebugBuiltInTracerConfig::CallTracer(CallConfig { ..Default::default() }),
        // );
        let options = GethDebugTracingOptions::default()
            .with_tracer(tracer)
            .with_call_config(CallConfig::default());
        let (block, txs, traces) =
            self.geth_debug_trace_block(block_number, options, include_transaction_hashes).await?;

        let mut calls = Vec::new();
        for trace in traces.into_iter() {
            // match trace {
            //     GethTrace::Known(GethTraceFrame::CallTracer(call_frame)) =>
            // calls.push(call_frame),     _ => return
            // Err(CollectError::CollectError("invalid trace result".to_string())), }
            match trace {
                TraceResult::Success { result, tx_hash } => match result {
                    GethTrace::CallTracer(frame) => calls.push(frame),
                    _ => {
                        return Err(CollectError::CollectError(format!(
                            "invalid trace result in tx {:?}",
                            tx_hash
                        )))
                    }
                },
                TraceResult::Error { error, tx_hash } => {
                    return Err(CollectError::CollectError(format!(
                        "invalid trace result in tx {:?}: {}",
                        tx_hash, error
                    )));
                }
            }
        }
        Ok((block, txs, calls))
    }

    /// get geth debug block diff traces
    pub async fn geth_debug_trace_block_diffs(
        &self,
        block_number: u32,
        include_transaction_hashes: bool,
    ) -> Result<(Option<u32>, Vec<Option<Vec<u8>>>, Vec<DiffMode>)> {
        let tracer = GethDebugTracerType::BuiltInTracer(GethDebugBuiltInTracerType::PreStateTracer);
        // let config = GethDebugTracerConfig::BuiltInTracer(
        //     GethDebugBuiltInTracerConfig::PreStateTracer(PreStateConfig { diff_mode: Some(true)
        // }),
        let options = GethDebugTracingOptions::default()
            .with_prestate_config(PreStateConfig {
                diff_mode: Some(true),
                disable_code: None,
                disable_storage: None,
            })
            .with_tracer(tracer);
        let (block, txs, traces) =
            self.geth_debug_trace_block(block_number, options, include_transaction_hashes).await?;

        let mut diffs = Vec::new();
        for trace in traces.into_iter() {
            match trace {
                TraceResult::Success { result, tx_hash } => match result {
                    GethTrace::PreStateTracer(PreStateFrame::Diff(diff)) => diffs.push(diff),
                    GethTrace::JS(serde_json::Value::Object(map)) => {
                        let diff = parse_geth_diff_object(map)?;
                        diffs.push(diff);
                    }
                    _ => {
                        println!("{:?}", result);
                        return Err(CollectError::CollectError(format!(
                            "invalid trace result in tx {:?}",
                            tx_hash
                        )));
                    }
                },
                TraceResult::Error { error, tx_hash } => {
                    return Err(CollectError::CollectError(format!(
                        "invalid trace result in tx {:?}: {}",
                        tx_hash, error
                    )));
                }
            }
        }
        Ok((block, txs, diffs))
    }

    /// get geth debug transaction traces
    pub async fn geth_debug_trace_transaction(
        &self,
        transaction_hash: Vec<u8>,
        options: GethDebugTracingOptions,
        include_block_number: bool,
    ) -> Result<(Option<u32>, Vec<Option<Vec<u8>>>, Vec<GethTrace>)> {
        let ethers_tx = B256::from_slice(&transaction_hash);

        let trace = {
            let _permit = self.permit_request().await;
            self.provider
                .debug_trace_transaction(ethers_tx, options)
                .await
                .map_err(CollectError::ProviderError)?
        };
        let traces = vec![trace];

        let block_number = if include_block_number {
            match self.get_transaction_by_hash(ethers_tx).await? {
                Some(tx) => tx.block_number.map(|x| x as u32),
                None => {
                    return Err(CollectError::CollectError(
                        "could not get block for txs".to_string(),
                    ))
                }
            }
        } else {
            None
        };

        Ok((block_number, vec![Some(transaction_hash)], traces))
    }

    /// get geth debug block javascript traces
    pub async fn geth_debug_trace_transaction_javascript_traces(
        &self,
        js_tracer: String,
        transaction_hash: Vec<u8>,
        include_block_number: bool,
    ) -> Result<(Option<u32>, Vec<Option<Vec<u8>>>, Vec<serde_json::Value>)> {
        let tracer = GethDebugTracerType::JsTracer(js_tracer);
        let options = GethDebugTracingOptions { tracer: Some(tracer), ..Default::default() };
        let (block, txs, traces) = self
            .geth_debug_trace_transaction(transaction_hash, options, include_block_number)
            .await?;

        let mut calls = Vec::new();
        for trace in traces.into_iter() {
            match trace {
                GethTrace::JS(value) => calls.push(value),
                _ => return Err(CollectError::CollectError("invalid trace result".to_string())),
            }
        }
        Ok((block, txs, calls))
    }

    /// get geth debug block opcode traces
    pub async fn geth_debug_trace_transaction_opcodes(
        &self,
        transaction_hash: Vec<u8>,
        include_block_number: bool,
        options: GethDebugTracingOptions,
    ) -> Result<(Option<u32>, Vec<Option<Vec<u8>>>, Vec<DefaultFrame>)> {
        let (block, txs, traces) = self
            .geth_debug_trace_transaction(transaction_hash, options, include_block_number)
            .await?;

        let mut calls = Vec::new();
        for trace in traces.into_iter() {
            match trace {
                GethTrace::Default(frame) => calls.push(frame),
                _ => return Err(CollectError::CollectError("invalid trace result".to_string())),
            }
        }
        Ok((block, txs, calls))
    }

    /// get geth debug block 4byte traces
    pub async fn geth_debug_trace_transaction_4byte_traces(
        &self,
        transaction_hash: Vec<u8>,
        include_block_number: bool,
    ) -> Result<(Option<u32>, Vec<Option<Vec<u8>>>, Vec<BTreeMap<String, u64>>)> {
        let tracer = GethDebugTracerType::BuiltInTracer(GethDebugBuiltInTracerType::FourByteTracer);
        let options = GethDebugTracingOptions { tracer: Some(tracer), ..Default::default() };
        let (block, txs, traces) = self
            .geth_debug_trace_transaction(transaction_hash, options, include_block_number)
            .await?;

        let mut calls = Vec::new();
        for trace in traces.into_iter() {
            match trace {
                GethTrace::FourByteTracer(frame) => calls.push(frame.0),
                _ => return Err(CollectError::CollectError("invalid trace result".to_string())),
            }
        }
        Ok((block, txs, calls))
    }

    /// get geth debug block call traces
    pub async fn geth_debug_trace_transaction_prestate(
        &self,
        transaction_hash: Vec<u8>,
        include_block_number: bool,
    ) -> Result<(Option<u32>, Vec<Option<Vec<u8>>>, Vec<BTreeMap<Address, AccountState>>)> {
        let tracer = GethDebugTracerType::BuiltInTracer(GethDebugBuiltInTracerType::PreStateTracer);
        let options = GethDebugTracingOptions { tracer: Some(tracer), ..Default::default() };
        let (block, txs, traces) = self
            .geth_debug_trace_transaction(transaction_hash, options, include_block_number)
            .await?;

        let mut calls = Vec::new();
        for trace in traces.into_iter() {
            match trace {
                GethTrace::PreStateTracer(PreStateFrame::Default(frame)) => calls.push(frame.0),
                _ => return Err(CollectError::CollectError("invalid trace result".to_string())),
            }
        }
        Ok((block, txs, calls))
    }

    /// get geth debug block call traces
    pub async fn geth_debug_trace_transaction_calls(
        &self,
        transaction_hash: Vec<u8>,
        include_block_number: bool,
    ) -> Result<(Option<u32>, Vec<Option<Vec<u8>>>, Vec<CallFrame>)> {
        let tracer = GethDebugTracerType::BuiltInTracer(GethDebugBuiltInTracerType::CallTracer);
        // let config = GethDebugTracerConfig::BuiltInTracer(
        //     GethDebugBuiltInTracerConfig::CallTracer(CallConfig { ..Default::default() }),
        // );
        let options = GethDebugTracingOptions::default()
            .with_tracer(tracer)
            .with_call_config(CallConfig::default());
        let (block, txs, traces) = self
            .geth_debug_trace_transaction(transaction_hash, options, include_block_number)
            .await?;

        let mut calls = Vec::new();
        for trace in traces.into_iter() {
            match trace {
                // GethTrace::Known(GethTraceFrame::CallTracer(call_frame)) =>
                // calls.push(call_frame),
                GethTrace::CallTracer(frame) => calls.push(frame),
                _ => return Err(CollectError::CollectError("invalid trace result".to_string())),
            }
        }
        Ok((block, txs, calls))
    }

    /// get geth debug block diff traces
    pub async fn geth_debug_trace_transaction_diffs(
        &self,
        transaction_hash: Vec<u8>,
        include_transaction_hashes: bool,
    ) -> Result<(Option<u32>, Vec<Option<Vec<u8>>>, Vec<DiffMode>)> {
        let tracer = GethDebugTracerType::BuiltInTracer(GethDebugBuiltInTracerType::PreStateTracer);
        // let config = GethDebugTracerConfig::BuiltInTracer(
        //     GethDebugBuiltInTracerConfig::PreStateTracer(PreStateConfig { diff_mode: Some(true)
        // }), );
        let options = GethDebugTracingOptions::default().with_tracer(tracer).with_prestate_config(
            PreStateConfig { diff_mode: Some(true), disable_code: None, disable_storage: None },
        );
        let (block, txs, traces) = self
            .geth_debug_trace_transaction(transaction_hash, options, include_transaction_hashes)
            .await?;

        let mut diffs = Vec::new();
        for trace in traces.into_iter() {
            match trace {
                // GethTrace::Known(GethTraceFrame::PreStateTracer(PreStateFrame::Diff(diff))) => {
                //     diffs.push(diff)
                // }
                GethTrace::PreStateTracer(PreStateFrame::Diff(diff)) => diffs.push(diff),
                _ => return Err(CollectError::CollectError("invalid trace result".to_string())),
            }
        }
        Ok((block, txs, diffs))
    }

    async fn permit_request(
        &self,
    ) -> Option<::core::result::Result<SemaphorePermit<'_>, AcquireError>> {
        let permit = match &*self.semaphore {
            Some(semaphore) => Some(semaphore.acquire().await),
            _ => None,
        };
        if let Some(limiter) = &*self.rate_limiter {
            limiter.until_ready().await;
        }
        permit
    }

    fn map_err<T>(res: ::core::result::Result<T, RpcError<TransportErrorKind>>) -> Result<T> {
        res.map_err(CollectError::ProviderError)
    }
}

use crate::err;
use std::collections::BTreeMap;

fn parse_geth_diff_object(map: serde_json::Map<String, serde_json::Value>) -> Result<DiffMode> {
    let pre: BTreeMap<Address, AccountState> = serde_json::from_value(map["pre"].clone())
        .map_err(|_| err("cannot deserialize pre diff"))?;
    let post: BTreeMap<Address, AccountState> = serde_json::from_value(map["post"].clone())
        .map_err(|_| err("cannot deserialize pre diff"))?;

    Ok(DiffMode { pre, post })
}
