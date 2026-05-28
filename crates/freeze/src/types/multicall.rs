//! Multicall3 helpers for batching `eth_call` invocations.
//!
//! [Multicall3](https://www.multicall3.com) is a thin aggregator contract that
//! exposes `aggregate3(Call3[])` — given a list of `(target, allowFailure,
//! callData)` triples it returns a parallel list of `(success, returnData)`
//! results in one RPC round-trip. On almost every supported chain it lives
//! at the canonical CREATE2 address `0xcA11bde05977b3631167028862bE2a173976CA11`;
//! zkSync-family chains have a different CREATE2 implementation and need an
//! address override (see [`multicall3_info`]).
//!
//! The module exposes:
//! - The [`Multicall3`] `sol!` binding (one function: `aggregate3`).
//! - [`Multicall3Info`] + [`multicall3_info`] — per-chain (address, deploy block).
//! - The [`MulticallBatchable`] trait + [`multicall_collect_by_block`] runner so any
//!   `CollectByBlock` dataset can opt into batched extraction with ~30 lines.
//! - [`default_collect_by_block`] — extracted from `CollectByBlock`'s default impl so per-dataset
//!   `collect_by_block` overrides can fall through to it when the user hasn't opted into multicall.
//! - [`decode_string_or_bytes32`] — length-aware decoder for ERC-20 name/symbol returns; covers
//!   both the standard `string` shape and the pre-standard `bytes32` shape used by MKR/SAI/DGD-era
//!   tokens.

use crate::{
    collect_generic::{fetch_partition, join_partition_handles},
    CollectByBlock, CollectError, Datatype, Params, Partition, Query, Source, ToDataFrames, R,
};
use alloy::{
    primitives::{address, Address, Bytes},
    sol,
    sol_types::{SolCall, SolValue},
};
use polars::prelude::*;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::mpsc;

/// Canonical Multicall3 deploy address on Ethereum and almost every L2.
///
/// zkSync-family chains use a different address — call [`multicall3_info`] for
/// chain-aware dispatch instead of using this constant directly.
pub const MULTICALL3_ADDRESS: Address = address!("cA11bde05977b3631167028862bE2a173976CA11");

/// Default batch size when the caller doesn't specify one.
///
/// **Best-effort cap on inner eth_calls per Multicall3 transaction** (see
/// [`rows_per_batch`] — row count is divided by `calls_per_row`, so this caps
/// inner calls rather than rows). The cap is best-effort: a single row is never
/// split, so a dataset whose `calls_per_row` exceeds `batch_size` still ships
/// one whole row per multicall (i.e. `calls_per_row` inner calls). No dataset
/// in cryo today approaches that — the max is `erc20_metadata` at 3 — so the
/// cap holds in practice. At 250 calls × ~5k gas per balanceOf-shape inner =
/// ~1.25M gas, well under the 30M block-gas cap on mainnet + every mainstream
/// L2.
///
/// Bumped from upstream's 150 to 250 (modest +66% to reduce multicall count
/// without triggering server-side timeouts at scale). Empirically 1000-batch
/// regressed against real RPCs at 1.9M-call workloads — the halving fallback
/// in [`multicall_batch_with_fallback`] kicked in and dragged wall-clock past
/// the smaller-batch baseline. Datasets with expensive inner calls
/// (on-chain SVG `tokenURI`, multi-statement state queries) override via
/// [`MulticallBatchable::default_multicall_batch_size`].
pub const DEFAULT_MULTICALL_BATCH_SIZE: u32 = 250;

/// Rows per Multicall3 batch, given the inner-call budget and per-row call count.
///
/// `batch_size` caps inner eth_calls per multicall; dividing by `calls_per_row`
/// converts that budget into a row count. Floored at 1 so that (a) a dataset
/// whose `calls_per_row` exceeds `batch_size` still makes progress — one whole
/// row per multicall — and (b) `slice::chunks`, which panics on a zero chunk
/// size, is never handed 0. `calls_per_row` is itself floored at 1 to keep the
/// division total even if a caller passes 0.
fn rows_per_batch(batch_size: usize, calls_per_row: usize) -> usize {
    (batch_size / calls_per_row.max(1)).max(1)
}

sol! {
    /// Minimal Multicall3 binding — `aggregate3` is the only function cryo needs.
    #[allow(missing_docs)]
    contract Multicall3 {
        struct Call3 {
            address target;
            bool allowFailure;
            bytes callData;
        }
        struct Result {
            bool success;
            bytes returnData;
        }
        function aggregate3(Call3[] calldata calls) external payable returns (Result[] memory returnData);
    }
}

/// Per-chain Multicall3 deployment metadata.
#[derive(Debug, Clone, Copy)]
pub struct Multicall3Info {
    /// Address Multicall3 is deployed at on this chain.
    pub address: Address,
    /// Block number at which Multicall3 was first deployed on this chain.
    pub deploy_block: u64,
}

/// Look up Multicall3 (address, deploy_block) for a given chain id.
///
/// Returns `None` for chains we have not verified. Callers should treat
/// `None` as "Multicall3 unavailable — fall back to per-call extraction".
///
/// Add new chains only after cross-checking deployment from the canonical
/// [deploys list](https://www.multicall3.com/deployments).
pub fn multicall3_info(chain_id: u64) -> Option<Multicall3Info> {
    // helper for readability
    const fn info(addr: Address, deploy_block: u64) -> Multicall3Info {
        Multicall3Info { address: addr, deploy_block }
    }
    match chain_id {
        1 => Some(info(MULTICALL3_ADDRESS, 14_353_601)), // Ethereum mainnet
        10 => Some(info(MULTICALL3_ADDRESS, 4_286_263)), // Optimism
        56 => Some(info(MULTICALL3_ADDRESS, 15_921_452)), // BNB Chain
        100 => Some(info(MULTICALL3_ADDRESS, 21_022_491)), // Gnosis
        137 => Some(info(MULTICALL3_ADDRESS, 25_770_160)), // Polygon
        8_453 => Some(info(MULTICALL3_ADDRESS, 5_022)),  // Base
        42_161 => Some(info(MULTICALL3_ADDRESS, 7_654_707)), // Arbitrum One
        43_114 => Some(info(MULTICALL3_ADDRESS, 11_907_934)), // Avalanche C-Chain
        // zkSync Era: different CREATE2 implementation -> different canonical address.
        324 => Some(info(address!("F9cda624FBC7e059355ce98a31693d299FACd963"), 3_908_235)),
        _ => None,
    }
}

/// Decode an ERC-20 name/symbol return value into `Option<String>`.
///
/// The early-era ERC-20 tokens (MKR `0x9f8F72aA9304c8B593d555F12eF6589cC3A579A2`,
/// SAI `0x89d24A6b4CcB1B6fAA2625fE562bDD9a23260359`, DGD
/// `0xE0B7927c4aF23765Cb51314A0E0521A9645F0E2A`, …) predate the spec finalization
/// and return `bytes32` rather than `string`. Dispatch on the wire size:
///
/// - exactly 32 bytes -> `bytes32`, trim trailing nulls + parse UTF-8
/// - ≥ 64 bytes -> `string` ABI layout (offset + length + padded data)
/// - anything else -> `None` (empty / truncated / self-destructed)
pub fn decode_string_or_bytes32(data: &[u8]) -> Option<String> {
    match data.len() {
        0 => None,
        32 => {
            let trimmed: Vec<u8> = data.iter().take_while(|&&b| b != 0).copied().collect();
            String::from_utf8(trimmed).ok().map(|s| remove_control_characters(&s))
        }
        len if len >= 64 => String::abi_decode(data).ok().map(|s| remove_control_characters(&s)),
        _ => None,
    }
}

/// Strip ASCII control characters and spaces from a decoded name/symbol.
///
/// Mirrors `erc20_metadata::remove_control_characters` so callers using
/// [`decode_string_or_bytes32`] get the same post-processing as the existing
/// per-call path.
pub fn remove_control_characters(s: &str) -> String {
    s.chars().filter(|c| !c.is_ascii_control() && *c != ' ').collect()
}

/// Trait for `CollectByBlock` datasets that can be extracted via Multicall3 batching.
///
/// Each implementor describes how to turn one [`Params`] row into a fixed
/// number of [`Multicall3::Call3`] entries, and how to decode the parallel
/// [`Multicall3::Result`] slice back into the dataset's `Response`.
///
/// The actual batching loop lives in [`multicall_collect_by_block`] — datasets
/// dispatch into it from their `CollectByBlock::collect_by_block` override.
pub trait MulticallBatchable: CollectByBlock {
    /// Build the Multicall3 inner-calls for a single row's params.
    ///
    /// The number returned must be constant per implementor — the runner
    /// records `calls.len()` for the first row and uses it to slice each
    /// row's results out of the flat `aggregate3` return array. Returning a
    /// variable count silently corrupts decoded output.
    ///
    /// # Errors
    /// Returns `Err` if required [`Params`] fields are missing (e.g. `address`,
    /// `contract`) — the runner propagates the error and falls back to per-call
    /// extraction for the offending row.
    fn calls_for_row(params: &Params, require_success: bool) -> R<Vec<Multicall3::Call3>>;

    /// Decode the slice of `Multicall3::Result` entries for one row into the
    /// dataset's `Response`.
    ///
    /// `results.len()` is guaranteed by the runner to equal whatever
    /// [`calls_for_row`] returned for the first row of the batch.
    ///
    /// # Errors
    /// Returns `Err` only for unrecoverable encoding bugs. Per-call reverts
    /// arrive as `Result { success: false, returnData: [] }` and should map to
    /// `None` in the response, not an error.
    fn decode_row(params: &Params, results: &[Multicall3::Result]) -> R<Self::Response>;

    /// Default batch size for this dataset.
    ///
    /// Override to lower the default when inner calls are expensive (e.g.
    /// `tokenURI(id)` for ERC-721 with on-chain SVG metadata). Reads
    /// `multicall_batch_size` from [`Query`] first if non-zero.
    fn default_multicall_batch_size() -> u32 {
        DEFAULT_MULTICALL_BATCH_SIZE
    }
}

/// Default per-call collection path — same shape as `CollectByBlock::collect_by_block`'s
/// default impl, extracted as a free function so per-dataset overrides can
/// fall through to it without duplicating the logic.
///
/// # Errors
/// Propagates any [`CollectError`] from partition expansion, RPC dispatch, or
/// the dataset's `transform` step.
pub async fn default_collect_by_block<D>(
    partition: Partition,
    source: Arc<Source>,
    query: Arc<Query>,
    inner_request_size: Option<u64>,
) -> R<HashMap<Datatype, DataFrame>>
where
    D: CollectByBlock + ToDataFrames,
{
    let (sender, receiver) = mpsc::channel(1);
    let chain_id = source.chain_id;
    let handles =
        fetch_partition(D::extract, partition, source, inner_request_size, query.clone(), sender)
            .await?;
    let columns = <D as CollectByBlock>::transform_channel(receiver, &query).await?;
    join_partition_handles(handles).await?;
    columns.create_dfs(&query.schemas, chain_id)
}

/// Multicall3-batched collection path for `D: MulticallBatchable`.
///
/// Groups params by block, sends one `aggregate3` per `batch_size` rows
/// (defaulting to [`MulticallBatchable::default_multicall_batch_size`] when
/// `Query::multicall_batch_size` is 0), with iterative halving on RPC error
/// and a final per-call fallback for singleton batches. Rows at blocks
/// earlier than the chain's Multicall3 deploy block — or on unknown chains —
/// route through [`CollectByBlock::extract`] directly so the schema is
/// preserved across the whole partition.
///
/// # Errors
/// Returns `Err` only for unrecoverable conditions (mpsc send failure,
/// dataset `transform` failure). Individual call reverts are surfaced as
/// `None` in the row's response per the dataset's `decode_row`.
pub async fn multicall_collect_by_block<D>(
    partition: Partition,
    source: Arc<Source>,
    query: Arc<Query>,
    inner_request_size: Option<u64>,
) -> R<HashMap<Datatype, DataFrame>>
where
    D: MulticallBatchable + Send + Sync + 'static,
    D::Response: Send + 'static,
{
    let (sender, receiver) = mpsc::channel(1);
    let chain_id = source.chain_id;
    let mc = multicall3_info(chain_id);
    let batch_size = if query.multicall_batch_size > 0 {
        query.multicall_batch_size
    } else {
        D::default_multicall_batch_size()
    } as usize;
    let batch_size = batch_size.max(1);
    let require_success = query.multicall_require_success;

    // Split params into multicall-eligible (grouped by block) vs ineligible
    // (per-call fallback for pre-deploy blocks / unknown chains).
    let params = partition.param_sets(inner_request_size)?;
    let mut by_block: HashMap<u64, Vec<Params>> = HashMap::new();
    let mut ineligible: Vec<Params> = Vec::new();
    for p in params {
        let block = p.block_number.unwrap_or(0);
        match mc {
            Some(info) if block >= info.deploy_block => by_block.entry(block).or_default().push(p),
            _ => ineligible.push(p),
        }
    }

    let mut handles = Vec::new();

    // Spawn one task per (block, batch) of multicall-eligible rows.
    if let Some(info) = mc {
        // `multicall_batch_size` is the cap on **inner eth_calls per Multicall3 tx**,
        // not on rows. Datasets like `erc20_metadata` produce 3 inner calls per row
        // (name + symbol + decimals), so at batch_size=300 we want ~100 rows per
        // multicall — not 300. We divide by the row's call count, peeking at the
        // first row in the block (each row's call count is constant per dataset for
        // every dataset in cryo today). See [`rows_per_batch`] for the floor-at-1 rule.
        //
        // Pre-fix this code chunked by `batch_size` rows directly. At batch_size=300
        // that meant 900 inner calls per multicall for metadata vs 300 for supplies
        // — 3× larger gas + payload, 3× the server-side EVM work per RTT, ~6× the
        // total wall-clock on a 700-contract scan (8s vs 1.3s in cdc-homie's smoke).
        for (block, params_for_block) in by_block {
            // `calls_per_row` is dataset-constant; deriving it from this block's
            // first row inside the loop avoids the prior pre-pass HashMap.
            let calls_per_row = params_for_block
                .first()
                .map(|p| {
                    D::calls_for_row(p, require_success).map(|cs| cs.len().max(1)).unwrap_or(1)
                })
                .unwrap_or(1);
            let effective_rows_per_batch = rows_per_batch(batch_size, calls_per_row);
            for chunk in params_for_block.chunks(effective_rows_per_batch) {
                let chunk = chunk.to_vec();
                let sender = sender.clone();
                let source = source.clone();
                let query = query.clone();
                let mc_address = info.address;
                let handle = tokio::task::spawn(async move {
                    let responses = multicall_batch_with_fallback::<D>(
                        block,
                        chunk,
                        &source,
                        query,
                        require_success,
                        mc_address,
                    )
                    .await?;
                    for resp in responses {
                        sender.send(Ok(resp)).await.map_err(|_| {
                            CollectError::CollectError("mpsc send failed".to_string())
                        })?;
                    }
                    Ok::<(), CollectError>(())
                });
                handles.push(handle);
            }
        }
    }

    // Spawn per-call task for each ineligible row.
    for p in ineligible {
        let sender = sender.clone();
        let source = source.clone();
        let query = query.clone();
        let handle = tokio::task::spawn(async move {
            let resp = D::extract(p, source, query).await?;
            sender
                .send(Ok(resp))
                .await
                .map_err(|_| CollectError::CollectError("mpsc send failed".to_string()))?;
            Ok::<(), CollectError>(())
        });
        handles.push(handle);
    }

    drop(sender);

    let columns = <D as CollectByBlock>::transform_channel(receiver, &query).await?;
    join_partition_handles(handles).await?;
    columns.create_dfs(&query.schemas, chain_id)
}

/// Iteratively dispatch `aggregate3` against the batch, halving on RPC failure
/// down to single-row batches. A single-row failure falls through to
/// `D::extract` so the row is preserved (per the dataset's own per-call decoder).
async fn multicall_batch_with_fallback<D>(
    block: u64,
    batch: Vec<Params>,
    source: &Arc<Source>,
    query: Arc<Query>,
    require_success: bool,
    mc_address: Address,
) -> R<Vec<D::Response>>
where
    D: MulticallBatchable,
{
    let mut stack: Vec<Vec<Params>> = vec![batch];
    let mut out: Vec<D::Response> = Vec::new();
    while let Some(current) = stack.pop() {
        match multicall_batch::<D>(block, &current, source, require_success, mc_address).await {
            Ok(responses) => out.extend(responses),
            Err(_) if current.len() > 1 => {
                let mid = current.len() / 2;
                let mut left = current;
                let right = left.split_off(mid);
                // Push right first so left is popped (and retried) next — keeps
                // produced rows in roughly input order; the final transform is
                // order-agnostic anyway.
                stack.push(right);
                stack.push(left);
            }
            Err(_) => {
                // Singleton batch failed at the RPC layer — fall through to
                // per-call extract so the row's `Response` matches the
                // dataset's existing semantics (rather than dropping the row).
                let p = current.into_iter().next().expect("len checked above");
                out.push(D::extract(p, source.clone(), query.clone()).await?);
            }
        }
    }
    Ok(out)
}

async fn multicall_batch<D>(
    block: u64,
    batch: &[Params],
    source: &Arc<Source>,
    require_success: bool,
    mc_address: Address,
) -> R<Vec<D::Response>>
where
    D: MulticallBatchable,
{
    // Build the flat aggregate3 call list and remember how many inner calls
    // each row contributed so we can slice the results back per row.
    let mut all_calls: Vec<Multicall3::Call3> = Vec::with_capacity(batch.len());
    let mut calls_per_row: Vec<usize> = Vec::with_capacity(batch.len());
    for p in batch {
        let row_calls = D::calls_for_row(p, require_success)?;
        calls_per_row.push(row_calls.len());
        all_calls.extend(row_calls);
    }

    let call_data = Multicall3::aggregate3Call { calls: all_calls }.abi_encode();
    let raw: Bytes = source
        .call2(mc_address, call_data, block)
        .await
        .map_err(|e| CollectError::CollectError(format!("multicall RPC failed: {e:?}")))?;
    let decoded = Multicall3::aggregate3Call::abi_decode_returns(&raw)
        .map_err(|e| CollectError::CollectError(format!("multicall decode failed: {e:?}")))?;

    let total_expected: usize = calls_per_row.iter().sum();
    if decoded.len() != total_expected {
        return Err(CollectError::CollectError(format!(
            "multicall returned {} results for {} expected (batch={} rows)",
            decoded.len(),
            total_expected,
            batch.len(),
        )))
    }

    let mut out = Vec::with_capacity(batch.len());
    let mut idx = 0;
    for (p, n) in batch.iter().zip(calls_per_row) {
        let slice = &decoded[idx..idx + n];
        out.push(D::decode_row(p, slice)?);
        idx += n;
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::rows_per_batch;

    #[test]
    fn divides_inner_call_budget_by_calls_per_row() {
        // 250-inner-call budget, 3 calls/row (erc20_metadata) ⇒ 83 rows (83×3 = 249 ≤ 250).
        assert_eq!(rows_per_batch(250, 3), 83);
        // 1 call/row (supplies / balances) ⇒ the whole budget as rows.
        assert_eq!(rows_per_batch(250, 1), 250);
        // exact division.
        assert_eq!(rows_per_batch(300, 3), 100);
    }

    #[test]
    fn floors_at_one_row() {
        // calls_per_row > batch_size ⇒ one whole row per multicall (never 0,
        // which would panic `slice::chunks`).
        assert_eq!(rows_per_batch(2, 3), 1);
        assert_eq!(rows_per_batch(1, 250), 1);
    }

    #[test]
    fn guards_zero_calls_per_row() {
        // Defensive: a 0 `calls_per_row` must not divide-by-zero.
        assert_eq!(rows_per_batch(250, 0), 250);
    }
}
