use crate::*;
use alloy::{
    primitives::{B256, U256},
    rpc::types::{Filter, Log, Topic},
    sol_types::SolEvent,
};
use polars::prelude::*;

/// columns for ERC-20 wrapper-style supply-modifying events.
///
/// Captures `Deposit(address indexed, uint256)` and
/// `Withdrawal(address indexed, uint256)` — the canonical WETH-shape events
/// that increment / decrement total supply alongside the standard ERC-20
/// `Transfer(0x0, x, v)` / `Transfer(x, 0x0, v)` mint/burn convention.
///
/// Many DeFi tokens (Compound cTokens, Aave aTokens — partially, MakerDAO
/// DSToken, generic ERC-4626 wrappers) reuse the same event signatures, so
/// indexing at the event-sig level catches them all without per-contract
/// configuration.
#[cryo_to_df::to_df(Datatype::Erc20WrapperEvents)]
#[derive(Default)]
pub struct Erc20WrapperEvents {
    n_rows: u64,
    block_number: Vec<u32>,
    block_hash: Vec<Option<Vec<u8>>>,
    transaction_index: Vec<u32>,
    log_index: Vec<u32>,
    transaction_hash: Vec<Vec<u8>>,
    erc20: Vec<Vec<u8>>,
    /// `"deposit"` (Deposit event) or `"withdrawal"` (Withdrawal event)
    event_type: Vec<String>,
    /// the indexed counterparty — `dst` for deposits, `src` for withdrawals
    account: Vec<Vec<u8>>,
    value: Vec<U256>,
    chain_id: Vec<u64>,
}

impl Dataset for Erc20WrapperEvents {
    fn aliases() -> Vec<&'static str> {
        vec!["wrapper_events", "weth_events"]
    }

    fn default_columns() -> Option<Vec<&'static str>> {
        Some(vec![
            "block_number",
            "transaction_index",
            "log_index",
            "transaction_hash",
            "erc20",
            "event_type",
            "account",
            "value",
            "chain_id",
        ])
    }

    fn optional_parameters() -> Vec<Dim> {
        // Topic0 is fixed (we filter to Deposit + Withdrawal sigs only); Topic1
        // narrows to a specific account if the user wants it. Topic2/3 don't
        // exist on these events (only one indexed arg).
        vec![Dim::Address, Dim::Topic1]
    }

    fn use_block_ranges() -> bool {
        true
    }

    fn default_inner_request_size() -> u64 {
        // Same default as logs / erc20_transfers — Deposit/Withdrawal volumes
        // are similar (WETH alone is several hundred events per block on
        // mainnet, and we accept the full superset across all emitting
        // contracts).
        50
    }

    fn arg_aliases() -> Option<std::collections::HashMap<Dim, Dim>> {
        Some([(Dim::Contract, Dim::Address)].into_iter().collect())
    }
}

impl CollectByBlock for Erc20WrapperEvents {
    type Response = Vec<Log>;

    async fn extract(request: Params, source: Arc<Source>, _: Arc<Query>) -> R<Self::Response> {
        let mut topics: [Topic; 4] = Default::default();
        // Union filter — eth_getLogs supports list per topic position (OR semantics).
        topics[0] =
            vec![ERC20Wrapper::Deposit::SIGNATURE_HASH, ERC20Wrapper::Withdrawal::SIGNATURE_HASH]
                .into();
        if let Some(account) = &request.topic1 {
            topics[1] = B256::from_slice(account).into();
        }
        let filter = Filter { topics, ..request.ethers_log_filter()? };
        let logs = source.get_logs(&filter).await?;
        // Shape filter: both events are `(address indexed, uint256)` → 2 topics
        // + 32-byte data. Drops malformed entries or contracts that happen to
        // emit other events with one of these topic0s but a different schema.
        Ok(logs.into_iter().filter(is_wrapper_event_shape).collect())
    }

    fn transform(response: Self::Response, columns: &mut Self, query: &Arc<Query>) -> R<()> {
        let schema = query.schemas.get_schema(&Datatype::Erc20WrapperEvents)?;
        process_wrapper_events(response, columns, schema)
    }
}

impl CollectByTransaction for Erc20WrapperEvents {
    type Response = Vec<Log>;

    async fn extract(request: Params, source: Arc<Source>, _: Arc<Query>) -> R<Self::Response> {
        let logs = source.get_transaction_logs(request.transaction_hash()?).await?;
        Ok(logs.into_iter().filter(is_wrapper_event).collect())
    }

    fn transform(response: Self::Response, columns: &mut Self, query: &Arc<Query>) -> R<()> {
        let schema = query.schemas.get_schema(&Datatype::Erc20WrapperEvents)?;
        process_wrapper_events(response, columns, schema)
    }
}

/// True iff `log` has the right topic count and data length for either
/// Deposit or Withdrawal (both share the `(address indexed,uint256)` shape).
fn is_wrapper_event_shape(log: &Log) -> bool {
    log.topics().len() == 2 && log.data().data.len() == 32
}

/// Per-transaction path: also verifies topic0 since this isn't pre-filtered
/// by `eth_getLogs` (we got *all* tx logs and need to pick out the wrapper ones).
fn is_wrapper_event(log: &Log) -> bool {
    is_wrapper_event_shape(log) &&
        log.topics().first().is_some_and(|t| {
            *t == ERC20Wrapper::Deposit::SIGNATURE_HASH ||
                *t == ERC20Wrapper::Withdrawal::SIGNATURE_HASH
        })
}

/// process logs into columns
fn process_wrapper_events(
    logs: Vec<Log>,
    columns: &mut Erc20WrapperEvents,
    schema: &Table,
) -> R<()> {
    for log in logs.iter() {
        let topic0 = log.topics().first().copied().unwrap_or_default();
        let event_type = if topic0 == ERC20Wrapper::Deposit::SIGNATURE_HASH {
            "deposit"
        } else if topic0 == ERC20Wrapper::Withdrawal::SIGNATURE_HASH {
            "withdrawal"
        } else {
            // Defensive — extract()'s union filter + shape check should already
            // have rejected this, but if a future caller reaches process_*
            // directly with mixed logs (e.g., via the coalesced LogEvents
            // multi-dataset), skip rather than mis-tag.
            continue;
        };

        if let (Some(bn), Some(tx), Some(ti), Some(li)) =
            (log.block_number, log.transaction_hash, log.transaction_index, log.log_index)
        {
            columns.n_rows += 1;
            store!(schema, columns, block_number, bn as u32);
            store!(schema, columns, block_hash, log.block_hash.map(|bh| bh.to_vec()));
            store!(schema, columns, transaction_index, ti as u32);
            store!(schema, columns, log_index, li as u32);
            store!(schema, columns, transaction_hash, tx.to_vec());
            store!(schema, columns, erc20, log.address().to_vec());
            store!(schema, columns, event_type, event_type.to_string());
            // topics[1] is the indexed `address` (padded to 32 bytes — low 20 are the address).
            store!(schema, columns, account, log.topics()[1][12..].to_vec());
            store!(
                schema,
                columns,
                value,
                U256::from_be_slice(log.data().data.to_vec().as_slice())
            );
        }
    }
    Ok(())
}
