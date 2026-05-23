use crate::{
    collect_generic::{fetch_partition, join_partition_handles},
    *,
};
use alloy::{
    primitives::{keccak256, Address, Bytes, TxKind},
    rpc::types::{TransactionInput, TransactionRequest},
    sol_types::SolCall,
};
use polars::prelude::*;
use std::collections::HashMap;
use tokio::sync::mpsc;

/// columns for transactions
#[cryo_to_df::to_df(Datatype::EthCalls)]
#[derive(Default)]
pub struct EthCalls {
    n_rows: u64,
    block_number: Vec<u32>,
    contract_address: Vec<Vec<u8>>,
    call_data: Vec<Vec<u8>>,
    call_data_hash: Vec<Vec<u8>>,
    output_data: Vec<Option<Vec<u8>>>,
    output_data_hash: Vec<Option<Vec<u8>>>,
    chain_id: Vec<u64>,
}

#[async_trait::async_trait]
impl Dataset for EthCalls {
    fn default_columns() -> Option<Vec<&'static str>> {
        Some(vec!["block_number", "contract_address", "call_data", "output_data", "chain_id"])
    }

    fn default_sort() -> Option<Vec<&'static str>> {
        Some(vec!["block_number", "contract_address"])
    }

    fn default_blocks() -> Option<String> {
        Some("latest".to_string())
    }

    fn arg_aliases() -> Option<std::collections::HashMap<Dim, Dim>> {
        Some([(Dim::Address, Dim::Contract), (Dim::ToAddress, Dim::Contract)].into_iter().collect())
    }

    fn required_parameters() -> Vec<Dim> {
        vec![Dim::Contract, Dim::CallData]
    }
}

type EthCallsResponse = (u32, Vec<u8>, Vec<u8>, Option<Vec<u8>>);

#[async_trait::async_trait]
impl CollectByBlock for EthCalls {
    type Response = EthCallsResponse;

    async fn extract(request: Params, source: Arc<Source>, _: Arc<Query>) -> R<Self::Response> {
        let response = single_eth_call(&request, &source).await?;
        Ok(response)
    }

    fn transform(response: Self::Response, columns: &mut Self, query: &Arc<Query>) -> R<()> {
        let schema = query.schemas.get_schema(&Datatype::EthCalls)?;
        process_eth_call(response, columns, schema);
        Ok(())
    }

    async fn collect_by_block(
        partition: Partition,
        source: Arc<Source>,
        query: Arc<Query>,
        inner_request_size: Option<u64>,
    ) -> R<HashMap<Datatype, DataFrame>> {
        if !query.multicall {
            // Inlined default per-call path so the trait override doesn't lose behaviour.
            let (sender, receiver) = mpsc::channel(1);
            let chain_id = source.chain_id;
            let handles = fetch_partition(
                <EthCalls as CollectByBlock>::extract,
                partition,
                source,
                inner_request_size,
                query.clone(),
                sender,
            )
            .await?;
            let columns = <EthCalls as CollectByBlock>::transform_channel(receiver, &query).await?;
            join_partition_handles(handles).await?;
            return columns.create_dfs(&query.schemas, chain_id);
        }
        multicall_collect_by_block(partition, source, query, inner_request_size).await
    }
}

impl CollectByTransaction for EthCalls {
    type Response = ();
}

async fn single_eth_call(request: &Params, source: &Arc<Source>) -> R<EthCallsResponse> {
    let transaction = TransactionRequest {
        to: Some(TxKind::Call(request.ethers_contract()?)),
        input: TransactionInput::new(request.call_data()?.into()),
        ..Default::default()
    };
    let number = request.block_number()?;
    let output = source.call(transaction, number).await.ok().map(|x| x.to_vec());
    Ok((number as u32, request.contract()?, request.call_data()?, output))
}

fn process_eth_call(response: EthCallsResponse, columns: &mut EthCalls, schema: &Table) {
    let (block_number, contract_address, call_data, output_data) = response;
    columns.n_rows += 1;
    store!(schema, columns, block_number, block_number);
    store!(schema, columns, contract_address, contract_address);
    store!(schema, columns, call_data, call_data.clone());
    store!(schema, columns, call_data_hash, keccak256(call_data).to_vec());
    store!(schema, columns, output_data, output_data.clone());
    store!(schema, columns, output_data_hash, output_data.map(|data| keccak256(data).to_vec()));
}

// ---------------------------------------------------------------------------
// Multicall3 batched collection path
// ---------------------------------------------------------------------------

async fn multicall_collect_by_block(
    partition: Partition,
    source: Arc<Source>,
    query: Arc<Query>,
    inner_request_size: Option<u64>,
) -> R<HashMap<Datatype, DataFrame>> {
    let (sender, receiver) = mpsc::channel(1);
    let chain_id = source.chain_id;
    let params = partition.param_sets(inner_request_size)?;
    let deploy_block = multicall3_deploy_block(chain_id);
    let batch_size = query.multicall_batch_size.max(1) as usize;
    let require_success = query.multicall_require_success;

    // Group eligible calls by block; route the rest through the per-call path.
    let mut by_block: HashMap<u64, Vec<Params>> = HashMap::new();
    let mut ineligible: Vec<Params> = Vec::new();
    for p in params {
        let block = p.block_number.unwrap_or(0);
        match deploy_block {
            Some(deploy) if block >= deploy => by_block.entry(block).or_default().push(p),
            _ => ineligible.push(p),
        }
    }

    let mut handles = Vec::new();

    for (block, calls) in by_block {
        for chunk in calls.chunks(batch_size) {
            let chunk = chunk.to_vec();
            let sender = sender.clone();
            let source = source.clone();
            let handle = tokio::task::spawn(async move {
                let responses =
                    multicall_batch_with_fallback(block, chunk, &source, require_success).await?;
                for resp in responses {
                    sender
                        .send(Ok(resp))
                        .await
                        .map_err(|_| CollectError::CollectError("mpsc send failed".to_string()))?;
                }
                Ok::<(), CollectError>(())
            });
            handles.push(handle);
        }
    }

    for p in ineligible {
        let sender = sender.clone();
        let source = source.clone();
        let handle = tokio::task::spawn(async move {
            let resp = single_eth_call(&p, &source).await?;
            sender
                .send(Ok(resp))
                .await
                .map_err(|_| CollectError::CollectError("mpsc send failed".to_string()))?;
            Ok::<(), CollectError>(())
        });
        handles.push(handle);
    }

    drop(sender);

    let columns = <EthCalls as CollectByBlock>::transform_channel(receiver, &query).await?;
    join_partition_handles(handles).await?;
    columns.create_dfs(&query.schemas, chain_id)
}

/// Iteratively dispatch `aggregate3` against the batch, halving on RPC failure
/// down to single calls. A single-call failure falls through to `eth_call`.
async fn multicall_batch_with_fallback(
    block: u64,
    batch: Vec<Params>,
    source: &Arc<Source>,
    require_success: bool,
) -> R<Vec<EthCallsResponse>> {
    let mut stack: Vec<Vec<Params>> = vec![batch];
    let mut out: Vec<EthCallsResponse> = Vec::new();
    while let Some(current) = stack.pop() {
        match multicall_batch(block, &current, source, require_success).await {
            Ok(responses) => out.extend(responses),
            Err(_) if current.len() > 1 => {
                let mid = current.len() / 2;
                let mut left = current;
                let right = left.split_off(mid);
                // Push right first so left is popped (and retried) next — preserves order
                // best-effort across halving, though the final transform step is order-agnostic.
                stack.push(right);
                stack.push(left);
            }
            Err(_) => {
                // single call inside aggregate3 failed at the RPC layer — fall back to a
                // direct eth_call so `output_data` becomes None rather than dropping the row.
                let p = current.into_iter().next().expect("len checked above");
                out.push(single_eth_call(&p, source).await?);
            }
        }
    }
    Ok(out)
}

async fn multicall_batch(
    block: u64,
    batch: &[Params],
    source: &Arc<Source>,
    require_success: bool,
) -> R<Vec<EthCallsResponse>> {
    let calls = batch
        .iter()
        .map(|p| {
            Ok::<_, CollectError>(Multicall3::Call3 {
                target: Address::from_slice(&p.contract()?),
                allowFailure: !require_success,
                callData: Bytes::from(p.call_data()?),
            })
        })
        .collect::<R<Vec<_>>>()?;

    let call_data = Multicall3::aggregate3Call { calls }.abi_encode();
    let raw = source
        .call2(MULTICALL3_ADDRESS, call_data, block)
        .await
        .map_err(|e| CollectError::CollectError(format!("multicall RPC failed: {e:?}")))?;
    let decoded = Multicall3::aggregate3Call::abi_decode_returns(&raw)
        .map_err(|e| CollectError::CollectError(format!("multicall decode failed: {e:?}")))?;

    if decoded.len() != batch.len() {
        return Err(CollectError::CollectError(format!(
            "multicall returned {} results for a {} call batch",
            decoded.len(),
            batch.len()
        )))
    }

    let mut out = Vec::with_capacity(batch.len());
    for (p, result) in batch.iter().zip(decoded) {
        let output_data = if result.success { Some(result.returnData.to_vec()) } else { None };
        out.push((p.block_number()? as u32, p.contract()?, p.call_data()?, output_data));
    }
    Ok(out)
}
