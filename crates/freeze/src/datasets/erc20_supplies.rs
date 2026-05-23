use crate::*;
use alloy::{
    primitives::{Address, Bytes, U256},
    sol_types::SolCall,
};
use polars::prelude::*;
use std::collections::HashMap;

/// columns for transactions
#[cryo_to_df::to_df(Datatype::Erc20Supplies)]
#[derive(Default)]
pub struct Erc20Supplies {
    n_rows: u64,
    block_number: Vec<u32>,
    erc20: Vec<Vec<u8>>,
    total_supply: Vec<Option<U256>>,
    chain_id: Vec<u64>,
}

impl Dataset for Erc20Supplies {
    fn default_sort() -> Option<Vec<&'static str>> {
        Some(vec!["erc20", "block_number"])
    }

    fn required_parameters() -> Vec<Dim> {
        vec![Dim::Address]
    }

    fn arg_aliases() -> Option<std::collections::HashMap<Dim, Dim>> {
        Some([(Dim::Contract, Dim::Address)].into_iter().collect())
    }

    fn default_blocks() -> Option<String> {
        Some("latest".to_string())
    }
}

impl CollectByBlock for Erc20Supplies {
    type Response = (u32, Vec<u8>, Option<U256>);

    async fn extract(request: Params, source: Arc<Source>, _: Arc<Query>) -> R<Self::Response> {
        let signature: Vec<u8> = ERC20::totalSupplyCall::SELECTOR.to_vec();
        let mut call_data = signature.clone();
        call_data.extend(request.address()?);
        let block_number = request.ethers_block_number()?;
        let contract = request.ethers_address()?;
        let output = source.call2(contract, call_data, block_number).await.ok();
        let output = output.map(|x| U256::from_be_slice(x.as_ref()));
        Ok((request.block_number()? as u32, request.address()?, output))
    }

    fn transform(response: Self::Response, columns: &mut Self, query: &Arc<Query>) -> R<()> {
        let schema = query.schemas.get_schema(&Datatype::Erc20Supplies)?;
        let (block, erc20, total_supply) = response;
        columns.n_rows += 1;
        store!(schema, columns, block_number, block);
        store!(schema, columns, erc20, erc20);
        store!(schema, columns, total_supply, total_supply);
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

impl CollectByTransaction for Erc20Supplies {
    type Response = ();
}

impl MulticallBatchable for Erc20Supplies {
    fn calls_for_row(params: &Params, require_success: bool) -> R<Vec<Multicall3::Call3>> {
        let target = Address::from_slice(&params.address()?);
        // totalSupply() takes no args; emit just the selector. (The legacy
        // per-call path above concatenates the address to the calldata —
        // harmless because extra calldata is ignored, but the batched path
        // does it correctly.)
        let call_data = ERC20::totalSupplyCall {}.abi_encode();
        Ok(vec![Multicall3::Call3 {
            target,
            allowFailure: !require_success,
            callData: Bytes::from(call_data),
        }])
    }

    fn decode_row(params: &Params, results: &[Multicall3::Result]) -> R<Self::Response> {
        let r = &results[0];
        let total_supply = if r.success && r.returnData.len() >= 32 {
            Some(U256::from_be_slice(&r.returnData[..32]))
        } else {
            None
        };
        Ok((params.block_number()? as u32, params.address()?, total_supply))
    }
}
