use crate::{types::rpc_params::fixed_from_slice, *};
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
/// Tokens beyond canonical WETH that emit these exact one-arg signatures —
/// WETH9 forks and WETH-style wrapped-native tokens across L2s — are caught
/// too, since indexing happens at the event-signature level with no
/// per-contract configuration. This does **not** include ERC-4626 vaults:
/// their `Deposit(sender, owner, assets, shares)` /
/// `Withdraw(sender, receiver, owner, assets, shares)` events are
/// multi-argument and hash to different topic0s, so they are not matched here.
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
            topics[1] = fixed_from_slice::<B256>(account, "topic1")?.into();
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

/// True iff `log` is a wrapper Deposit/Withdrawal: the shape check plus a topic0
/// match. The per-transaction path needs the topic0 check because (unlike the
/// by-block path) `eth_getLogs` didn't pre-filter the signature — we got *all*
/// tx logs and must pick out the wrapper ones. Shared with the coalesced
/// [`crate::LogEvents`] extractor's by-transaction fan-out.
pub(crate) fn is_wrapper_event(log: &Log) -> bool {
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
        // Shape guard first. The `account` column below reads `topics()[1]`, so
        // a log carrying a wrapper topic0 but fewer than 2 topics would panic.
        // A topic0-only check (the previous guard) does not cover that.
        // `is_wrapper_event` checks topic count, data width and topic0 together
        // — exactly this loop's precondition. Every current caller pre-filters;
        // this keeps a future caller that reaches process_* with mixed logs
        // (e.g. via the coalesced LogEvents multi-dataset) safe, skipping
        // rather than mis-tagging or aborting.
        if !is_wrapper_event(log) {
            continue;
        }
        let event_type = if log.topics()[0] == ERC20Wrapper::Deposit::SIGNATURE_HASH {
            "deposit"
        } else {
            // is_wrapper_event admits only Deposit or Withdrawal at topic0.
            "withdrawal"
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
            // topics[1] is the indexed `address` (padded to 32 bytes — low 20 are
            // the address). The `is_wrapper_event` guard above proves len == 2.
            store!(schema, columns, account, log.topics()[1][12..].to_vec());
            store!(schema, columns, value, U256::from_be_slice(&log.data().data));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::{Address, Bytes};

    /// real `Erc20WrapperEvents` schema, so `store!` actually evaluates each column
    fn schema() -> Table {
        Datatype::Erc20WrapperEvents
            .table_schema(
                &[U256Type::String],
                &ColumnEncoding::Hex,
                &None,
                &None,
                &None,
                None,
                None,
            )
            .expect("Erc20WrapperEvents has a valid default schema")
    }

    /// rpc `Log` carrying the block/tx/index fields `process_wrapper_events` requires
    fn log_with(topics: Vec<B256>, data_len: usize) -> Log {
        let inner = alloy::primitives::Log::new_unchecked(
            Address::ZERO,
            topics,
            Bytes::from(vec![0u8; data_len]),
        );
        Log {
            inner,
            block_number: Some(1),
            transaction_hash: Some(B256::ZERO),
            transaction_index: Some(0),
            log_index: Some(0),
            ..Default::default()
        }
    }

    #[test]
    fn wrapper_topic0_with_missing_topic1_is_skipped_not_panicking() {
        // Regression: the `account` column reads topics()[1]. A log with a
        // wrapper topic0 but only one topic passed the old topic0-only guard
        // and then panicked on the index. It must be skipped instead.
        let malformed = log_with(vec![ERC20Wrapper::Deposit::SIGNATURE_HASH], 32);
        assert!(!is_wrapper_event(&malformed));

        let mut columns = Erc20WrapperEvents::default();
        process_wrapper_events(vec![malformed], &mut columns, &schema()).unwrap();
        assert_eq!(columns.n_rows, 0, "malformed log must not produce a row");
    }

    #[test]
    fn wrapper_topic0_with_wrong_data_width_is_skipped() {
        // Deposit sig + 2 topics but 8-byte data ⇒ not a wrapper event.
        let malformed = log_with(vec![ERC20Wrapper::Deposit::SIGNATURE_HASH, B256::ZERO], 8);
        let mut columns = Erc20WrapperEvents::default();
        process_wrapper_events(vec![malformed], &mut columns, &schema()).unwrap();
        assert_eq!(columns.n_rows, 0);
    }

    #[test]
    fn well_formed_deposit_and_withdrawal_are_tagged() {
        let deposit = log_with(vec![ERC20Wrapper::Deposit::SIGNATURE_HASH, B256::ZERO], 32);
        let withdrawal = log_with(vec![ERC20Wrapper::Withdrawal::SIGNATURE_HASH, B256::ZERO], 32);

        let mut columns = Erc20WrapperEvents::default();
        process_wrapper_events(vec![deposit, withdrawal], &mut columns, &schema()).unwrap();

        assert_eq!(columns.n_rows, 2);
        assert_eq!(columns.event_type, vec!["deposit".to_string(), "withdrawal".to_string()]);
    }
}
