//! Coalesced `eth_getLogs` extractor.
//!
//! When the user requests two or more of `logs`, `erc20_transfers`,
//! `erc20_approvals`, `erc721_transfers` in the same crawl, the
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
//! The union filter is always at least as broad as every individual filter, so
//! every sub-dataset sees a strict superset of what it would have seen alone.
//! Each sub-dataset's transform then re-applies its own shape filter
//! (topic count + data length + topic0 match) before populating its columns —
//! mirroring the per-dataset behaviour today.
//!
//! - `address`: if any active sub-dataset has no address filter → union has no address restriction.
//!   Otherwise the address from `Params` (single user-supplied address per partition) is honoured.
//! - `topic0`: if `logs` is active AND the user passed no `--topic0` → union has no `topic0`
//!   restriction (raw logs wants everything). Otherwise the union is the list of `[user --topic0?,
//!   Transfer-sig?, Approval-sig?]` pruned by which sub-datasets are active.
//!
//! ERC20 Transfer and ERC721 Transfer share the same `topic0`
//! (`Transfer(address,address,uint256)` signature hash), so a single
//! topic-list slot covers both. The per-dataset shape filter (3 vs 4 topics)
//! disambiguates them at transform time.

use crate::{types::collection::*, *};
use alloy::{
    primitives::B256,
    rpc::types::{Filter, Log, Topic},
    sol_types::SolEvent,
};
use polars::prelude::*;
use std::collections::HashMap;

/// Coalesced log-events multi-dataset.
///
/// Holds one column accumulator per sub-dataset. Sub-accumulators for
/// datatypes the user did **not** request stay empty — their `create_dfs`
/// is a no-op because `query.schemas` won't carry their `Datatype` key.
#[derive(Default)]
pub struct LogEvents {
    /// raw logs accumulator (populated iff `query.schemas` includes `Datatype::Logs`)
    pub logs: Logs,
    /// erc20 transfers accumulator
    pub erc20_transfers: Erc20Transfers,
    /// erc20 approvals accumulator
    pub erc20_approvals: Erc20Approvals,
    /// erc721 transfers accumulator
    pub erc721_transfers: Erc721Transfers,
    /// erc20 wrapper events (Deposit / Withdrawal) accumulator
    pub erc20_wrapper_events: Erc20WrapperEvents,
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
/// Reads `query.schemas` to discover which sub-datasets are active, then
/// constructs a `Filter` that is a strict superset of each individual
/// dataset's filter. See module docs for the rule.
fn build_union_filter(request: &Params, query: &Query) -> R<Filter> {
    let want_logs = query.schemas.contains_key(&Datatype::Logs);
    let want_erc20_transfers = query.schemas.contains_key(&Datatype::Erc20Transfers);
    let want_erc20_approvals = query.schemas.contains_key(&Datatype::Erc20Approvals);
    let want_erc721_transfers = query.schemas.contains_key(&Datatype::Erc721Transfers);
    let want_erc20_wrapper_events = query.schemas.contains_key(&Datatype::Erc20WrapperEvents);

    let base = request.ethers_log_filter()?;

    // `logs` with no user-supplied topic0 means "all logs in the range" → can't
    // narrow further without dropping data. Return `base` unchanged (which also
    // carries any user `--address` / `--topic1..3` filters).
    if want_logs && request.topic0.is_none() {
        return Ok(base);
    }

    // Otherwise build the topic0 union from the active set.
    let mut topic0s: Vec<B256> = Vec::new();
    if let Some(user_topic0) = &request.topic0 {
        topic0s.push(B256::from_slice(user_topic0));
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

    if topic0s.is_empty() {
        return Ok(base);
    }

    let mut topics: [Topic; 4] = Default::default();
    topics[0] = topic0s.into();
    Ok(Filter { topics, ..base })
}

impl CollectByBlock for LogEvents {
    type Response = Vec<Log>;

    async fn extract(request: Params, source: Arc<Source>, query: Arc<Query>) -> R<Self::Response> {
        let filter = build_union_filter(&request, &query)?;
        source.get_logs(&filter).await
    }

    fn transform(response: Self::Response, columns: &mut Self, query: &Arc<Query>) -> R<()> {
        // Each sub-dataset re-filters the response by its own shape rule, so
        // the transform receives exactly the rows it would have on the
        // single-dataset path.
        let LogEvents {
            logs,
            erc20_transfers,
            erc20_approvals,
            erc721_transfers,
            erc20_wrapper_events,
        } = columns;

        if query.schemas.contains_key(&Datatype::Logs) {
            <Logs as CollectByBlock>::transform(response.clone(), logs, query)?;
        }
        if query.schemas.contains_key(&Datatype::Erc20Transfers) {
            let filtered: Vec<Log> = response
                .iter()
                .filter(|l| {
                    l.topics().first().is_some_and(|t| *t == ERC20::Transfer::SIGNATURE_HASH) &&
                        l.topics().len() == 3 &&
                        l.data().data.len() == 32
                })
                .cloned()
                .collect();
            <Erc20Transfers as CollectByBlock>::transform(filtered, erc20_transfers, query)?;
        }
        if query.schemas.contains_key(&Datatype::Erc20Approvals) {
            let filtered: Vec<Log> = response
                .iter()
                .filter(|l| {
                    l.topics().first().is_some_and(|t| *t == ERC20::Approval::SIGNATURE_HASH) &&
                        l.topics().len() == 3 &&
                        l.data().data.len() == 32
                })
                .cloned()
                .collect();
            <Erc20Approvals as CollectByBlock>::transform(filtered, erc20_approvals, query)?;
        }
        if query.schemas.contains_key(&Datatype::Erc721Transfers) {
            let filtered: Vec<Log> = response
                .iter()
                .filter(|l| {
                    l.topics().first().is_some_and(|t| *t == ERC721::Transfer::SIGNATURE_HASH) &&
                        l.topics().len() == 4 &&
                        l.data().data.is_empty()
                })
                .cloned()
                .collect();
            <Erc721Transfers as CollectByBlock>::transform(filtered, erc721_transfers, query)?;
        }
        if query.schemas.contains_key(&Datatype::Erc20WrapperEvents) {
            let filtered: Vec<Log> = response
                .iter()
                .filter(|l| {
                    let topic0 = l.topics().first();
                    let is_wrapper_sig = topic0.is_some_and(|t| {
                        *t == ERC20Wrapper::Deposit::SIGNATURE_HASH ||
                            *t == ERC20Wrapper::Withdrawal::SIGNATURE_HASH
                    });
                    is_wrapper_sig && l.topics().len() == 2 && l.data().data.len() == 32
                })
                .cloned()
                .collect();
            <Erc20WrapperEvents as CollectByBlock>::transform(
                filtered,
                erc20_wrapper_events,
                query,
            )?;
        }
        Ok(())
    }
}

impl CollectByTransaction for LogEvents {
    type Response = Vec<Log>;

    async fn extract(request: Params, source: Arc<Source>, _: Arc<Query>) -> R<Self::Response> {
        // Per-tx path: fetch all logs of the tx once, sub-datasets filter in transform.
        source.get_transaction_logs(request.transaction_hash()?).await
    }

    fn transform(response: Self::Response, columns: &mut Self, query: &Arc<Query>) -> R<()> {
        let LogEvents {
            logs,
            erc20_transfers,
            erc20_approvals,
            erc721_transfers,
            erc20_wrapper_events,
        } = columns;

        if query.schemas.contains_key(&Datatype::Logs) {
            <Logs as CollectByTransaction>::transform(response.clone(), logs, query)?;
        }
        if query.schemas.contains_key(&Datatype::Erc20Transfers) {
            let filtered: Vec<Log> = response
                .iter()
                .filter(|l| {
                    l.topics().first().is_some_and(|t| *t == ERC20::Transfer::SIGNATURE_HASH) &&
                        l.topics().len() == 3 &&
                        l.data().data.len() == 32
                })
                .cloned()
                .collect();
            <Erc20Transfers as CollectByTransaction>::transform(filtered, erc20_transfers, query)?;
        }
        if query.schemas.contains_key(&Datatype::Erc20Approvals) {
            let filtered: Vec<Log> = response
                .iter()
                .filter(|l| {
                    l.topics().first().is_some_and(|t| *t == ERC20::Approval::SIGNATURE_HASH) &&
                        l.topics().len() == 3 &&
                        l.data().data.len() == 32
                })
                .cloned()
                .collect();
            <Erc20Approvals as CollectByTransaction>::transform(filtered, erc20_approvals, query)?;
        }
        if query.schemas.contains_key(&Datatype::Erc721Transfers) {
            let filtered: Vec<Log> = response
                .iter()
                .filter(|l| {
                    l.topics().first().is_some_and(|t| *t == ERC721::Transfer::SIGNATURE_HASH) &&
                        l.topics().len() == 4 &&
                        l.data().data.is_empty()
                })
                .cloned()
                .collect();
            <Erc721Transfers as CollectByTransaction>::transform(
                filtered,
                erc721_transfers,
                query,
            )?;
        }
        if query.schemas.contains_key(&Datatype::Erc20WrapperEvents) {
            let filtered: Vec<Log> = response
                .iter()
                .filter(|l| {
                    let topic0 = l.topics().first();
                    let is_wrapper_sig = topic0.is_some_and(|t| {
                        *t == ERC20Wrapper::Deposit::SIGNATURE_HASH ||
                            *t == ERC20Wrapper::Withdrawal::SIGNATURE_HASH
                    });
                    is_wrapper_sig && l.topics().len() == 2 && l.data().data.len() == 32
                })
                .cloned()
                .collect();
            <Erc20WrapperEvents as CollectByTransaction>::transform(
                filtered,
                erc20_wrapper_events,
                query,
            )?;
        }
        Ok(())
    }
}
