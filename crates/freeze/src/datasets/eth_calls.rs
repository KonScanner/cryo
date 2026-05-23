use crate::*;
use alloy::{
    primitives::{keccak256, Address, Bytes, TxKind},
    rpc::types::{TransactionInput, TransactionRequest},
};
use polars::prelude::*;
use std::collections::HashMap;

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
        single_eth_call(&request, &source).await
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
        if query.multicall {
            multicall_collect_by_block::<Self>(partition, source, query, inner_request_size).await
        } else {
            default_collect_by_block::<Self>(partition, source, query, inner_request_size).await
        }
    }
}

impl CollectByTransaction for EthCalls {
    type Response = ();
}

impl MulticallBatchable for EthCalls {
    fn calls_for_row(params: &Params, require_success: bool) -> R<Vec<Multicall3::Call3>> {
        Ok(vec![Multicall3::Call3 {
            target: Address::from_slice(&params.contract()?),
            allowFailure: !require_success,
            callData: Bytes::from(params.call_data()?),
        }])
    }

    fn decode_row(params: &Params, results: &[Multicall3::Result]) -> R<Self::Response> {
        let r = &results[0];
        let output_data =
            if r.success && !r.returnData.is_empty() { Some(r.returnData.to_vec()) } else { None };
        Ok((params.block_number()? as u32, params.contract()?, params.call_data()?, output_data))
    }
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
