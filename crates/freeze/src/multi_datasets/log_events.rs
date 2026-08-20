//! Coalesced `eth_getLogs` extractor.
//!
//! When the user requests two or more of `logs`, `erc20_transfers`,
//! `erc20_approvals`, `erc721_transfers`, `erc20_wrapper_events` in the same crawl, the
//! [`crate::types::datatypes::cluster_datatypes`] planner clusters them into
//! `MultiDatatype::LogEvents`. This module's [`LogEvents`] struct then issues
//! **one** `eth_getLogs` per partition with a UNION filter and fans the
//! response out to each requested sub-dataset's column accumulator.
//!
//! Single-dataset behaviour is unchanged — the planner only routes through
//! here when ≥2 log-shaped datatypes are co-requested.
//!
//! ### Union filter rule
//!
//! The union filter must be a strict **superset** of every active member's
//! individual `eth_getLogs` filter, so that no member loses rows it would have
//! seen alone. It narrows on the shared block range and `--address`, on a
//! `topic0` union, and on `topic1..3` only where every active member pins the
//! same value there (see [`narrow_agreed_topics`]).
//!
//! `topic1..3` need that agreement test because members interpret those
//! positions **differently**:
//!
//! - raw `logs` maps `--topic1..3` straight onto log topic positions;
//! - `erc20_transfers` / `erc20_approvals` / `erc721_transfers` put `--from-address` /
//!   `--to-address` into topic1 / topic2;
//! - `erc20_wrapper_events` puts the indexed account into topic1.
//!
//! Narrowing a position that only *some* members constrain would under-collect
//! for the rest. Regardless of whether a position survives the agreement test,
//! each member re-applies its **full** single-dataset filter in [`fan_out_block`]
//! (see the `matches_*` predicates) against the partition's [`Params`], so every
//! accumulator receives exactly the rows it would have on its scalar path. That
//! client-side re-filter — not the union — is what preserves the "single-dataset
//! behaviour is unchanged" invariant; the union narrowing is purely a transport
//! optimisation that keeps a co-request from degrading into a range-wide scan.
//!
//! - `address`: honoured uniformly (every log dataset applies it via [`Params::ethers_log_filter`],
//!   and a partition carries a single address).
//! - `topic0`: if `logs` is active AND the user passed no `--topic0` → union has no `topic0`
//!   restriction (raw logs wants everything). Otherwise the union is the list of `[user --topic0?,
//!   Transfer-sig?, Approval-sig?, Deposit/Withdrawal-sig?]` pruned by which sub-datasets are
//!   active.
//!
//! ERC20 Transfer and ERC721 Transfer share the same `topic0`
//! (`Transfer(address,address,uint256)` signature hash), so a single
//! topic-list slot covers both. The per-dataset shape filter (3 vs 4 topics)
//! disambiguates them at transform time.
//!
//! ### By-transaction path
//!
//! The by-transaction path fetches all logs of a tx with no server-side filter,
//! exactly like every scalar `*-by-transaction` extractor; those extractors
//! apply only a shape+signature filter (never from/to/topic narrowing) and raw
//! `logs` stores every log. So the tx path re-applies just the shared
//! shape+signature predicates ([`is_erc20_transfer`], [`is_erc20_approval`],
//! [`is_erc721_transfer`], [`is_wrapper_event`]) — no
//! `Params` narrowing — which already matches single-dataset behaviour.

use crate::{
    datasets::{
        erc20_approvals::is_erc20_approval, erc20_transfers::is_erc20_transfer,
        erc20_wrapper_events::is_wrapper_event, erc721_transfers::is_erc721_transfer,
    },
    types::{collection::*, rpc_params::fixed_from_slice},
    *,
};
use alloy::{
    primitives::B256,
    rpc::types::{Filter, Log, Topic},
    sol_types::SolEvent,
};
use polars::prelude::*;
use std::collections::{HashMap, HashSet};

/// Coalesced log-events multi-dataset.
///
/// Holds one column accumulator per sub-dataset. Sub-accumulators for
/// datatypes the user did **not** request stay empty and MUST NOT be handed to
/// `create_dfs`: the `to_df`-generated impl does `schemas.get(&datatype)` and
/// panics on a missing key, so the `contains_key` guards below are load-bearing.
#[derive(Default)]
pub struct LogEvents {
    /// raw logs accumulator (populated iff `query.schemas` includes `Datatype::Logs`)
    logs: Logs,
    /// erc20 transfers accumulator
    erc20_transfers: Erc20Transfers,
    /// erc20 approvals accumulator
    erc20_approvals: Erc20Approvals,
    /// erc721 transfers accumulator
    erc721_transfers: Erc721Transfers,
    /// erc20 wrapper events (Deposit / Withdrawal) accumulator
    erc20_wrapper_events: Erc20WrapperEvents,
}

impl ToDataFrames for LogEvents {
    fn create_dfs(
        self,
        schemas: &HashMap<Datatype, Table>,
        chain_id: u64,
    ) -> R<HashMap<Datatype, DataFrame>> {
        let LogEvents {
            logs,
            erc20_transfers,
            erc20_approvals,
            erc721_transfers,
            erc20_wrapper_events,
        } = self;
        let mut output = HashMap::new();
        if schemas.contains_key(&Datatype::Logs) {
            output.extend(logs.create_dfs(schemas, chain_id)?);
        }
        if schemas.contains_key(&Datatype::Erc20Transfers) {
            output.extend(erc20_transfers.create_dfs(schemas, chain_id)?);
        }
        if schemas.contains_key(&Datatype::Erc20Approvals) {
            output.extend(erc20_approvals.create_dfs(schemas, chain_id)?);
        }
        if schemas.contains_key(&Datatype::Erc721Transfers) {
            output.extend(erc721_transfers.create_dfs(schemas, chain_id)?);
        }
        if schemas.contains_key(&Datatype::Erc20WrapperEvents) {
            output.extend(erc20_wrapper_events.create_dfs(schemas, chain_id)?);
        }
        Ok(output)
    }
}

/// Build the UNION filter for one partition chunk.
///
/// `active` is the set of requested datatypes (the keys of `query.schemas`).
/// Constructs a `Filter` that is a strict superset of each individual dataset's
/// filter. See module docs for the rule.
fn build_union_filter(request: &Params, active: &HashSet<Datatype>) -> R<Filter> {
    let want_logs = active.contains(&Datatype::Logs);
    let want_erc20_transfers = active.contains(&Datatype::Erc20Transfers);
    let want_erc20_approvals = active.contains(&Datatype::Erc20Approvals);
    let want_erc721_transfers = active.contains(&Datatype::Erc721Transfers);
    let want_erc20_wrapper_events = active.contains(&Datatype::Erc20WrapperEvents);

    // `base` supplies the shared block range + `--address`. We rebuild its
    // topic array below: topic0 becomes a union, and topic1..3 are re-derived by
    // `narrow_agreed_topics` (kept only where every member pins the same value,
    // since members disagree on their meaning — see module docs). Each member
    // re-applies its own full filter in `fan_out_block` either way.
    let base = request.ethers_log_filter()?;

    // Raw `logs` with no user-supplied `--topic0` means "every topic0 in the
    // range" → the union cannot narrow topic0 at all. Block range + address
    // still apply, and topic1..3 still get the agreement test.
    if want_logs && request.topic0.is_none() {
        let mut topics: [Topic; 4] = Default::default();
        narrow_agreed_topics(&mut topics, request, active);
        return Ok(Filter { topics, ..base });
    }

    // Otherwise build the topic0 union from the active set.
    let mut topic0s: Vec<B256> = Vec::new();
    if let Some(user_topic0) = &request.topic0 {
        topic0s.push(fixed_from_slice::<B256>(user_topic0, "topic0")?);
    }
    // ERC20 Transfer and ERC721 Transfer share the same SIGNATURE_HASH —
    // include once if either is active.
    if want_erc20_transfers || want_erc721_transfers {
        let h = ERC20::Transfer::SIGNATURE_HASH;
        if !topic0s.contains(&h) {
            topic0s.push(h);
        }
    }
    if want_erc20_approvals {
        let h = ERC20::Approval::SIGNATURE_HASH;
        if !topic0s.contains(&h) {
            topic0s.push(h);
        }
    }
    if want_erc20_wrapper_events {
        for h in [ERC20Wrapper::Deposit::SIGNATURE_HASH, ERC20Wrapper::Withdrawal::SIGNATURE_HASH] {
            if !topic0s.contains(&h) {
                topic0s.push(h);
            }
        }
    }

    let mut topics: [Topic; 4] = Default::default();
    if !topic0s.is_empty() {
        topics[0] = topic0s.into();
    }
    narrow_agreed_topics(&mut topics, request, active);
    Ok(Filter { topics, ..base })
}

/// The value member `dt` pins at topic position `pos` on its scalar path, or
/// `None` when that member leaves the position unconstrained.
///
/// This is the per-member half of [`narrow_agreed_topics`]; it must stay in step
/// with each member's scalar `extract` and with the `matches_*` predicates below.
fn pinned_topic(dt: Datatype, pos: usize, p: &Params) -> Option<&Vec<u8>> {
    match (dt, pos) {
        (Datatype::Logs, 1) => p.topic1.as_ref(),
        (Datatype::Logs, 2) => p.topic2.as_ref(),
        (Datatype::Logs, 3) => p.topic3.as_ref(),
        (Datatype::Erc20Transfers | Datatype::Erc20Approvals | Datatype::Erc721Transfers, 1) => {
            p.from_address.as_ref()
        }
        (Datatype::Erc20Transfers | Datatype::Erc20Approvals | Datatype::Erc721Transfers, 2) => {
            p.to_address.as_ref()
        }
        (Datatype::Erc20WrapperEvents, 1) => p.topic1.as_ref(),
        _ => None,
    }
}

/// Narrow `topics[1..=3]` at every position where **all** active members pin the
/// same value on their scalar paths.
///
/// The union has to stay a superset of each member's own filter, so a position is
/// narrowed only when no active member leaves it unconstrained and all of them
/// agree on the value. That keeps the superset invariant while avoiding the
/// pathological case the blanket strip created: co-requesting two datasets that
/// both filter on `--from-address` degraded two narrow `eth_getLogs` into one
/// unfiltered range scan, tripping provider result caps that neither scalar run
/// hits. `fan_out_block` still re-applies every predicate, so narrowing here is
/// purely a transport optimisation — it can never change which rows a member gets.
fn narrow_agreed_topics(topics: &mut [Topic; 4], request: &Params, active: &HashSet<Datatype>) {
    let members: Vec<Datatype> =
        MultiDatatype::LogEvents.datatypes().into_iter().filter(|dt| active.contains(dt)).collect();
    if members.is_empty() {
        return;
    }
    for (pos, slot) in topics.iter_mut().enumerate().skip(1) {
        // Compare the left-padded form so a 20-byte `--from-address` and a
        // 32-byte `--topic1` naming the same account still count as agreeing.
        let mut pinned = members
            .iter()
            .map(|dt| pinned_topic(*dt, pos, request).and_then(|v| address_dim_as_topic(v)));
        // Any `None` means a member is unconstrained here (or its value is too
        // wide to be a topic), so the union must leave the position open.
        let Some(Some(first)) = pinned.next() else { continue };
        if pinned.all(|t| t == Some(first)) {
            *slot = first.into();
        }
    }
}

/// True iff `log`'s topic at `idx` equals `want` (`None` ⇒ unconstrained).
///
/// For the `--topic0..3` dims only, where the scalar path compares the user's
/// bytes to a topic verbatim. A non-32-byte value cannot equal any topic and so
/// matches nothing here — but it never reaches this point, because
/// `build_union_filter`'s `request.ethers_log_filter()?` already rejected it.
fn topic_matches(log: &Log, idx: usize, want: &Option<Vec<u8>>) -> bool {
    match want {
        None => true,
        Some(bytes) => log.topics().get(idx).is_some_and(|t| t.as_slice() == bytes.as_slice()),
    }
}

/// Like [`topic_matches`], but for the address-shaped dims (`--from-address`,
/// `--to-address`), which the CLI stores at whatever width the user typed.
///
/// Compares against the left-padded 32-byte form, which is exactly what the
/// scalar extractors put into their RPC filter via [`address_dim_as_topic`]. A
/// value wider than 32 bytes matches nothing; the scalar path errors on the same
/// input, so neither returns rows.
fn address_topic_matches(log: &Log, idx: usize, want: &Option<Vec<u8>>) -> bool {
    match want {
        None => true,
        Some(bytes) => {
            address_dim_as_topic(bytes).is_some_and(|t| log.topics().get(idx) == Some(&t))
        }
    }
}

// --- full by-block predicates: each scalar dataset's shape+signature predicate
// (imported from its own module — the single source of truth) plus the
// per-position narrowing that dataset applies at the RPC. The union filter
// intentionally omits this narrowing (see module docs), so re-applying it here
// is what keeps each member's output identical to its single-dataset run. ---

/// Raw `logs` by-block filter: the user `--topic0..3` narrowing that
/// `Params::ethers_log_filter` applies at the RPC on the scalar path. (Raw logs
/// has no shape constraint — it stores every matching log.)
fn matches_logs(log: &Log, params: &Params) -> bool {
    topic_matches(log, 0, &params.topic0) &&
        topic_matches(log, 1, &params.topic1) &&
        topic_matches(log, 2, &params.topic2) &&
        topic_matches(log, 3, &params.topic3)
}

/// `erc20_transfers` by-block: shape + `--from-address`/`--to-address`
/// (topic1/topic2). User `--topic1..3` are ignored, matching the scalar path.
fn matches_erc20_transfer(log: &Log, params: &Params) -> bool {
    is_erc20_transfer(log) &&
        address_topic_matches(log, 1, &params.from_address) &&
        address_topic_matches(log, 2, &params.to_address)
}

/// `erc20_approvals` by-block: shape + `--from-address`/`--to-address`.
fn matches_erc20_approval(log: &Log, params: &Params) -> bool {
    is_erc20_approval(log) &&
        address_topic_matches(log, 1, &params.from_address) &&
        address_topic_matches(log, 2, &params.to_address)
}

/// `erc721_transfers` by-block: shape + `--from-address`/`--to-address`.
fn matches_erc721_transfer(log: &Log, params: &Params) -> bool {
    is_erc721_transfer(log) &&
        address_topic_matches(log, 1, &params.from_address) &&
        address_topic_matches(log, 2, &params.to_address)
}

/// `erc20_wrapper_events` by-block: shape + the indexed account (topic1, set
/// from `--topic1` on the scalar path). `--address` / `--contract` selects the
/// emitting token contract instead, and the union filter's `base` applies it once.
fn matches_erc20_wrapper(log: &Log, params: &Params) -> bool {
    is_wrapper_event(log) && topic_matches(log, 1, &params.topic1)
}

/// Fan one by-block partition's union response out to each requested
/// sub-dataset, re-applying that sub-dataset's full scalar filter against
/// `request`. Each accumulator therefore receives exactly the rows it would
/// have collected on its single-dataset path.
fn fan_out_block(
    request: &Params,
    response: Vec<Log>,
    columns: &mut LogEvents,
    query: &Arc<Query>,
) -> R<()> {
    let LogEvents {
        logs,
        erc20_transfers,
        erc20_approvals,
        erc721_transfers,
        erc20_wrapper_events,
    } = columns;

    // The four narrow members borrow `response`; raw `logs` — typically the
    // largest slice of it — consumes it last, so the biggest member costs no
    // clone at all.
    if query.schemas.contains_key(&Datatype::Erc20Transfers) {
        let rows: Vec<Log> =
            response.iter().filter(|l| matches_erc20_transfer(l, request)).cloned().collect();
        <Erc20Transfers as CollectByBlock>::transform(rows, erc20_transfers, query)?;
    }
    if query.schemas.contains_key(&Datatype::Erc20Approvals) {
        let rows: Vec<Log> =
            response.iter().filter(|l| matches_erc20_approval(l, request)).cloned().collect();
        <Erc20Approvals as CollectByBlock>::transform(rows, erc20_approvals, query)?;
    }
    if query.schemas.contains_key(&Datatype::Erc721Transfers) {
        let rows: Vec<Log> =
            response.iter().filter(|l| matches_erc721_transfer(l, request)).cloned().collect();
        <Erc721Transfers as CollectByBlock>::transform(rows, erc721_transfers, query)?;
    }
    if query.schemas.contains_key(&Datatype::Erc20WrapperEvents) {
        let rows: Vec<Log> =
            response.iter().filter(|l| matches_erc20_wrapper(l, request)).cloned().collect();
        <Erc20WrapperEvents as CollectByBlock>::transform(rows, erc20_wrapper_events, query)?;
    }
    if query.schemas.contains_key(&Datatype::Logs) {
        let rows: Vec<Log> = response.into_iter().filter(|l| matches_logs(l, request)).collect();
        <Logs as CollectByBlock>::transform(rows, logs, query)?;
    }
    Ok(())
}

/// Fan one by-transaction partition's logs out to each requested sub-dataset.
/// The tx path has no server-side filter and no `Params` narrowing on any
/// scalar dataset, so each member re-applies only its shape+signature predicate
/// (raw `logs` takes every log).
fn fan_out_transaction(response: &[Log], columns: &mut LogEvents, query: &Arc<Query>) -> R<()> {
    let LogEvents {
        logs,
        erc20_transfers,
        erc20_approvals,
        erc721_transfers,
        erc20_wrapper_events,
    } = columns;

    if query.schemas.contains_key(&Datatype::Logs) {
        <Logs as CollectByTransaction>::transform(response.to_vec(), logs, query)?;
    }
    if query.schemas.contains_key(&Datatype::Erc20Transfers) {
        let rows: Vec<Log> = response.iter().filter(|l| is_erc20_transfer(l)).cloned().collect();
        <Erc20Transfers as CollectByTransaction>::transform(rows, erc20_transfers, query)?;
    }
    if query.schemas.contains_key(&Datatype::Erc20Approvals) {
        let rows: Vec<Log> = response.iter().filter(|l| is_erc20_approval(l)).cloned().collect();
        <Erc20Approvals as CollectByTransaction>::transform(rows, erc20_approvals, query)?;
    }
    if query.schemas.contains_key(&Datatype::Erc721Transfers) {
        let rows: Vec<Log> = response.iter().filter(|l| is_erc721_transfer(l)).cloned().collect();
        <Erc721Transfers as CollectByTransaction>::transform(rows, erc721_transfers, query)?;
    }
    if query.schemas.contains_key(&Datatype::Erc20WrapperEvents) {
        let rows: Vec<Log> = response.iter().filter(|l| is_wrapper_event(l)).cloned().collect();
        <Erc20WrapperEvents as CollectByTransaction>::transform(rows, erc20_wrapper_events, query)?;
    }
    Ok(())
}

impl CollectByBlock for LogEvents {
    // Carry the partition's `Params` alongside the logs so `transform` can
    // re-apply each member's full single-dataset filter (the union filter
    // intentionally drops topic1..3 / from-to narrowing — see module docs).
    type Response = (Params, Vec<Log>);

    async fn extract(request: Params, source: Arc<Source>, query: Arc<Query>) -> R<Self::Response> {
        let active: HashSet<Datatype> = query.schemas.keys().cloned().collect();
        let filter = build_union_filter(&request, &active)?;
        let logs = source.get_logs(&filter).await?;
        Ok((request, logs))
    }

    fn transform(response: Self::Response, columns: &mut Self, query: &Arc<Query>) -> R<()> {
        let (request, logs) = response;
        fan_out_block(&request, logs, columns, query)
    }
}

impl CollectByTransaction for LogEvents {
    type Response = Vec<Log>;

    async fn extract(request: Params, source: Arc<Source>, _: Arc<Query>) -> R<Self::Response> {
        // Per-tx path: fetch all logs of the tx once, sub-datasets filter in transform.
        source.get_transaction_logs(request.transaction_hash()?).await
    }

    fn transform(response: Self::Response, columns: &mut Self, query: &Arc<Query>) -> R<()> {
        fan_out_transaction(&response, columns, query)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::{Address, Bytes};

    /// minimal rpc `Log` with the given topics and `data_len` zero-bytes of data
    fn rpc_log(topics: Vec<B256>, data_len: usize) -> Log {
        let inner = alloy::primitives::Log::new_unchecked(
            Address::ZERO,
            topics,
            Bytes::from(vec![0u8; data_len]),
        );
        Log { inner, ..Default::default() }
    }

    /// `rpc_log` plus the block/tx/log identity fields every `process_*` requires
    /// before it will emit a row.
    fn located_log(topics: Vec<B256>, data_len: usize, log_index: u64) -> Log {
        Log {
            block_number: Some(1),
            transaction_hash: Some(B256::repeat_byte(0x77)),
            transaction_index: Some(0),
            log_index: Some(log_index),
            ..rpc_log(topics, data_len)
        }
    }

    /// a `Query` carrying just the schemas the fan-out consults
    fn test_query(datatypes: &[Datatype]) -> Arc<Query> {
        let schemas = datatypes
            .iter()
            .map(|dt| {
                let table = dt
                    .table_schema(
                        &[U256Type::Binary],
                        &ColumnEncoding::Binary,
                        &None,
                        &None,
                        &None,
                        None,
                        None,
                    )
                    .unwrap();
                (*dt, table)
            })
            .collect();
        Arc::new(Query {
            datatypes: vec![MetaDatatype::Multi(MultiDatatype::LogEvents)],
            schemas,
            time_dimension: TimeDimension::Blocks,
            partitions: vec![],
            partitioned_by: vec![],
            exclude_failed: false,
            js_tracer: None,
            labels: QueryLabels { align: false, reorg_buffer: 0 },
            multicall: false,
            multicall_batch_size: 0,
            multicall_require_success: false,
        })
    }

    fn height(dfs: &HashMap<Datatype, DataFrame>, dt: Datatype) -> usize {
        dfs.get(&dt).map(|df| df.height()).unwrap_or(0)
    }

    /// the set of requested datatypes, as `build_union_filter` expects
    fn active(datatypes: &[Datatype]) -> HashSet<Datatype> {
        datatypes.iter().cloned().collect()
    }

    #[test]
    fn raw_logs_filter_drops_foreign_topic0() {
        // H1 regression: with `--topic0 X`, the coalesced raw `logs` member must
        // NOT keep a co-requested sibling's rows (e.g. Transfer-sig logs that
        // entered the shared response via the union filter).
        let x = B256::repeat_byte(0xab);
        let params = Params { topic0: Some(x.to_vec()), ..Default::default() };

        let x_log = rpc_log(vec![x], 32);
        let transfer_log =
            rpc_log(vec![ERC20::Transfer::SIGNATURE_HASH, B256::ZERO, B256::ZERO], 32);

        assert!(matches_logs(&x_log, &params), "topic0==X log must be kept");
        assert!(!matches_logs(&transfer_log, &params), "foreign topic0 must be dropped");
    }

    #[test]
    fn raw_logs_filter_unconstrained_keeps_everything() {
        // No `--topic0..3` ⇒ raw logs keeps every row (matches scalar behaviour).
        let params = Params::default();
        let any = rpc_log(vec![B256::repeat_byte(0x11)], 0);
        assert!(matches_logs(&any, &params));
    }

    #[test]
    fn erc20_transfer_honors_from_address() {
        // Regression: `--from-address` must narrow the coalesced erc20_transfers
        // (the union filter drops it, so the per-member re-filter must apply it).
        let from = B256::repeat_byte(0xcd);
        let other = B256::repeat_byte(0xee);
        let params = Params { from_address: Some(from.to_vec()), ..Default::default() };

        let sig = ERC20::Transfer::SIGNATURE_HASH;
        let matching = rpc_log(vec![sig, from, B256::ZERO], 32);
        let nonmatching = rpc_log(vec![sig, other, B256::ZERO], 32);

        assert!(matches_erc20_transfer(&matching, &params));
        assert!(!matches_erc20_transfer(&nonmatching, &params));
    }

    #[test]
    fn transfer_shape_disambiguates_erc20_from_erc721() {
        // ERC20 and ERC721 Transfer share `topic0`; the topic count splits them.
        assert_eq!(ERC20::Transfer::SIGNATURE_HASH, ERC721::Transfer::SIGNATURE_HASH);
        let sig = ERC20::Transfer::SIGNATURE_HASH;
        let erc20 = rpc_log(vec![sig, B256::ZERO, B256::ZERO], 32);
        let erc721 = rpc_log(vec![sig, B256::ZERO, B256::ZERO, B256::ZERO], 0);

        assert!(is_erc20_transfer(&erc20));
        assert!(!is_erc20_transfer(&erc721));
        assert!(is_erc721_transfer(&erc721));
        assert!(!is_erc721_transfer(&erc20));
    }

    #[test]
    fn union_filter_unions_topic0_and_strips_lower_topics() {
        // logs + erc20_transfers + user --topic0 X + --topic1: the union must
        // carry {X, Transfer_sig} at topic0 and DROP topic1..3 (erc20_transfers
        // puts from-address in topic1, so a topic1 in the union under-collects it).
        let x = B256::repeat_byte(0xab);
        let params = Params {
            block_range: Some((0, 10)),
            topic0: Some(x.to_vec()),
            topic1: Some(B256::repeat_byte(0x01).to_vec()),
            ..Default::default()
        };
        let filter =
            build_union_filter(&params, &active(&[Datatype::Logs, Datatype::Erc20Transfers]))
                .unwrap();

        let expected_topic0: Topic = vec![x, ERC20::Transfer::SIGNATURE_HASH].into();
        assert_eq!(filter.topics[0], expected_topic0);
        assert!(filter.topics[1].is_empty(), "user --topic1 must be stripped from the union");
        assert!(filter.topics[2].is_empty());
        assert!(filter.topics[3].is_empty());
    }

    #[test]
    fn malformed_user_topic0_errors_instead_of_panicking() {
        // Regression: `--topic0` is hex-decoded by the CLI with no length
        // check, so a short value reaches here as raw bytes. It must surface
        // as a typed error, not a `FixedBytes::from_slice` panic that aborts
        // the crawl mid-flight.
        let params = Params {
            block_range: Some((0, 10)),
            topic0: Some(vec![0xde, 0xad, 0xbe, 0xef]), // 4 bytes, not 32
            ..Default::default()
        };
        let err = build_union_filter(&params, &active(&[Datatype::Logs, Datatype::Erc20Transfers]))
            .unwrap_err();
        assert!(
            err.to_string().contains("topic0 must be 32 bytes, got 4"),
            "expected a width error, got: {err}"
        );
    }

    #[test]
    fn union_filter_unconstrained_when_logs_without_topic0() {
        // Raw `logs` with no --topic0 wants every topic0 ⇒ the union cannot
        // narrow topic0; topic1 is still stripped so siblings aren't narrowed.
        let params = Params {
            block_range: Some((0, 10)),
            topic1: Some(B256::repeat_byte(0x01).to_vec()),
            ..Default::default()
        };
        let filter =
            build_union_filter(&params, &active(&[Datatype::Logs, Datatype::Erc20Transfers]))
                .unwrap();
        assert!(filter.topics[0].is_empty(), "raw logs + no --topic0 ⇒ no topic0 constraint");
        assert!(
            filter.topics[1].is_empty(),
            "topic1 stripped so erc20_transfers isn't under-collected"
        );
    }

    #[test]
    fn erc20_approvals_by_transaction_transform_is_implemented() {
        // Regression: `impl CollectByTransaction for Erc20Approvals` used to define
        // only `extract`, so the trait default `transform` returned Err. On the
        // coalesced tx path that `?` killed EVERY co-requested member's output.
        let query = test_query(&[Datatype::Erc20Approvals]);
        let response =
            vec![located_log(vec![ERC20::Approval::SIGNATURE_HASH, B256::ZERO, B256::ZERO], 32, 0)];
        let mut cols = Erc20Approvals::default();
        <Erc20Approvals as CollectByTransaction>::transform(response, &mut cols, &query).unwrap();
        let dfs = cols.create_dfs(&query.schemas, 1).unwrap();
        assert_eq!(height(&dfs, Datatype::Erc20Approvals), 1);
    }

    #[test]
    fn erc20_transfer_honors_unpadded_cli_from_address() {
        // The CLI stores `--from-address` at the 20-byte width the user typed; the
        // scalar extractor left-pads it into topic1, so the coalesced predicate must
        // too. Comparing raw bytes matched nothing and wrote an empty parquet.
        let addr20 = vec![0xcdu8; 20];
        let padded = B256::left_padding_from(&addr20);
        let params = Params { from_address: Some(addr20), ..Default::default() };

        let sig = ERC20::Transfer::SIGNATURE_HASH;
        assert!(matches_erc20_transfer(&rpc_log(vec![sig, padded, B256::ZERO], 32), &params));
        assert!(!matches_erc20_transfer(
            &rpc_log(vec![sig, B256::repeat_byte(0xee), B256::ZERO], 32),
            &params
        ));
    }

    #[test]
    fn oversized_from_address_matches_nothing_instead_of_panicking() {
        let params = Params { from_address: Some(vec![0xab; 33]), ..Default::default() };
        let log = rpc_log(vec![ERC20::Transfer::SIGNATURE_HASH, B256::ZERO, B256::ZERO], 32);
        assert!(!matches_erc20_transfer(&log, &params));
    }

    #[test]
    fn union_filter_keeps_topic1_when_all_active_members_agree() {
        // erc20_transfers and erc20_approvals both pin from-address at topic1, so
        // the union can carry it — otherwise two narrow getLogs degrade into one
        // range-wide scan that trips provider result caps.
        let from = B256::repeat_byte(0xcd);
        let params = Params {
            block_range: Some((0, 10)),
            from_address: Some(from.to_vec()),
            ..Default::default()
        };
        let filter = build_union_filter(
            &params,
            &active(&[Datatype::Erc20Transfers, Datatype::Erc20Approvals]),
        )
        .unwrap();
        let expected: Topic = vec![from].into();
        assert_eq!(filter.topics[1], expected);
    }

    #[test]
    fn union_filter_drops_topic1_when_a_member_disagrees() {
        // `logs` and `erc20_wrapper_events` read topic1 as `--topic1`, which is unset
        // here — they constrain nothing, so narrowing would under-collect them.
        let from = B256::repeat_byte(0xcd);
        let params = Params {
            block_range: Some((0, 10)),
            from_address: Some(from.to_vec()),
            ..Default::default()
        };
        for extra in [Datatype::Logs, Datatype::Erc20WrapperEvents] {
            let filter =
                build_union_filter(&params, &active(&[Datatype::Erc20Transfers, extra])).unwrap();
            assert!(filter.topics[1].is_empty(), "{extra:?} must force topic1 unconstrained");
        }
    }

    #[test]
    fn union_filter_unions_wrapper_signatures_when_logs_inactive() {
        // Coverage gap: both existing union tests used [Logs, Erc20Transfers], so the
        // approvals arm and the wrapper two-signature loop carried no assertion.
        let params = Params { block_range: Some((0, 10)), ..Default::default() };
        let filter = build_union_filter(
            &params,
            &active(&[Datatype::Erc20Approvals, Datatype::Erc20WrapperEvents]),
        )
        .unwrap();
        let expected: Topic = vec![
            ERC20::Approval::SIGNATURE_HASH,
            ERC20Wrapper::Deposit::SIGNATURE_HASH,
            ERC20Wrapper::Withdrawal::SIGNATURE_HASH,
        ]
        .into();
        assert_eq!(filter.topics[0], expected);
    }

    #[test]
    fn fan_out_block_routes_each_row_to_exactly_its_member() {
        // The invariant the whole coalescing change rests on: one mixed response,
        // fanned out, gives every member exactly the rows its scalar run would.
        let transfer = ERC20::Transfer::SIGNATURE_HASH;
        let approval = ERC20::Approval::SIGNATURE_HASH;
        let deposit = ERC20Wrapper::Deposit::SIGNATURE_HASH;

        let response = vec![
            located_log(vec![transfer, B256::ZERO, B256::ZERO], 32, 0), // erc20 transfer
            located_log(vec![transfer, B256::ZERO, B256::ZERO, B256::ZERO], 0, 1), // erc721
            located_log(vec![approval, B256::ZERO, B256::ZERO], 32, 2), // approval
            located_log(vec![deposit, B256::ZERO], 32, 3),              // wrapper deposit
            located_log(vec![B256::repeat_byte(0xfe)], 0, 4),           // unrelated
        ];

        let datatypes = [
            Datatype::Logs,
            Datatype::Erc20Transfers,
            Datatype::Erc20Approvals,
            Datatype::Erc721Transfers,
            Datatype::Erc20WrapperEvents,
        ];
        let query = test_query(&datatypes);
        let mut columns = LogEvents::default();
        fan_out_block(&Params::default(), response, &mut columns, &query).unwrap();
        let dfs = columns.create_dfs(&query.schemas, 1).unwrap();

        assert_eq!(height(&dfs, Datatype::Logs), 5, "raw logs keeps every row");
        assert_eq!(height(&dfs, Datatype::Erc20Transfers), 1);
        assert_eq!(height(&dfs, Datatype::Erc20Approvals), 1);
        assert_eq!(height(&dfs, Datatype::Erc721Transfers), 1);
        assert_eq!(height(&dfs, Datatype::Erc20WrapperEvents), 1);
    }

    #[test]
    fn fan_out_block_skips_members_absent_from_the_schema_set() {
        // Only erc20_transfers requested: the other accumulators must stay empty and
        // must not be handed to `create_dfs`, whose generated impl panics on a
        // missing schema key.
        let query = test_query(&[Datatype::Erc20Transfers]);
        let response = vec![
            located_log(vec![ERC20::Transfer::SIGNATURE_HASH, B256::ZERO, B256::ZERO], 32, 0),
            located_log(vec![ERC20::Approval::SIGNATURE_HASH, B256::ZERO, B256::ZERO], 32, 1),
        ];
        let mut columns = LogEvents::default();
        fan_out_block(&Params::default(), response, &mut columns, &query).unwrap();
        let dfs = columns.create_dfs(&query.schemas, 1).unwrap();
        assert_eq!(dfs.len(), 1);
        assert_eq!(height(&dfs, Datatype::Erc20Transfers), 1);
    }
}
