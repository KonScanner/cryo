use crate::*;
use alloy::{
    primitives::{Address, Bytes, U256},
    sol_types::SolCall,
};
use polars::prelude::*;
use std::collections::HashMap;

/// columns for transactions
#[cryo_to_df::to_df(Datatype::Erc20Balances)]
#[derive(Default)]
pub struct Erc20Balances {
    n_rows: u64,
    block_number: Vec<u32>,
    erc20: Vec<Vec<u8>>,
    address: Vec<Vec<u8>>,
    balance: Vec<Option<U256>>,
    chain_id: Vec<u64>,
}

#[async_trait::async_trait]
impl Dataset for Erc20Balances {
    fn default_blocks() -> Option<String> {
        Some("latest".to_string())
    }

    fn required_parameters() -> Vec<Dim> {
        vec![Dim::Contract, Dim::Address]
    }
}

#[async_trait::async_trait]
impl CollectByBlock for Erc20Balances {
    type Response = (u32, Vec<u8>, Vec<u8>, Option<U256>);

    async fn extract(request: Params, source: Arc<Source>, _: Arc<Query>) -> R<Self::Response> {
        let signature = ERC20::balanceOfCall::SELECTOR;
        let mut call_data = signature.clone().to_vec();
        call_data.extend(vec![0; 12]);
        call_data.extend(request.address()?);
        let block_number = request.ethers_block_number()?;
        let contract = request.ethers_contract()?;
        let balance = source.call2(contract, call_data, block_number).await.ok();
        let balance = balance.map(|x| U256::from_be_slice(x.as_ref()));
        Ok((request.block_number()? as u32, request.contract()?, request.address()?, balance))
    }

    fn transform(response: Self::Response, columns: &mut Self, query: &Arc<Query>) -> R<()> {
        let schema = query.schemas.get_schema(&Datatype::Erc20Balances)?;
        let (block, erc20, address, balance) = response;
        columns.n_rows += 1;
        store!(schema, columns, block_number, block);
        store!(schema, columns, erc20, erc20);
        store!(schema, columns, address, address);
        store!(schema, columns, balance, balance);
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

impl CollectByTransaction for Erc20Balances {
    type Response = ();
}

impl MulticallBatchable for Erc20Balances {
    fn calls_for_row(params: &Params, require_success: bool) -> R<Vec<Multicall3::Call3>> {
        let owner = Address::from_slice(&params.address()?);
        let contract = Address::from_slice(&params.contract()?);
        let call_data = ERC20::balanceOfCall { owner }.abi_encode();
        Ok(vec![Multicall3::Call3 {
            target: contract,
            allowFailure: !require_success,
            callData: Bytes::from(call_data),
        }])
    }

    fn decode_row(params: &Params, results: &[Multicall3::Result]) -> R<Self::Response> {
        let r = &results[0];
        let balance = if r.success && r.returnData.len() >= 32 {
            Some(U256::from_be_slice(&r.returnData[..32]))
        } else {
            None
        };
        Ok((params.block_number()? as u32, params.contract()?, params.address()?, balance))
    }
}
