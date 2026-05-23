use crate::*;
use alloy::{
    primitives::{Address, Bytes},
    sol_types::SolCall,
};
use polars::prelude::*;
use std::collections::HashMap;

/// columns for transactions
#[cryo_to_df::to_df(Datatype::Erc20Metadata)]
#[derive(Default)]
pub struct Erc20Metadata {
    n_rows: u64,
    block_number: Vec<u32>,
    erc20: Vec<Vec<u8>>,
    name: Vec<Option<String>>,
    symbol: Vec<Option<String>>,
    decimals: Vec<Option<u32>>,
    chain_id: Vec<u64>,
}

impl Dataset for Erc20Metadata {
    fn default_sort() -> Option<Vec<&'static str>> {
        Some(vec!["symbol", "block_number"])
    }

    fn default_blocks() -> Option<String> {
        Some("latest".to_string())
    }

    fn required_parameters() -> Vec<Dim> {
        vec![Dim::Address]
    }

    fn arg_aliases() -> Option<std::collections::HashMap<Dim, Dim>> {
        Some([(Dim::Contract, Dim::Address)].into_iter().collect())
    }
}

pub(crate) fn remove_control_characters(s: &str) -> String {
    let re = regex::Regex::new(r"[ \x00-\x1F\x7F]").unwrap();
    re.replace_all(s, "").to_string()
}

impl CollectByBlock for Erc20Metadata {
    type Response = (u32, Vec<u8>, Option<String>, Option<String>, Option<u32>);

    async fn extract(request: Params, source: Arc<Source>, _: Arc<Query>) -> R<Self::Response> {
        let block_number = request.ethers_block_number()?;
        let address = request.ethers_address()?;

        // name
        let call_data = ERC20::nameCall::SELECTOR.to_vec();
        let name = match source.call2(address, call_data, block_number).await {
            Ok(output) => {
                String::from_utf8(output.to_vec()).ok().map(|s| remove_control_characters(&s))
            }
            Err(_) => None,
        };

        // symbol
        let call_data = ERC20::symbolCall::SELECTOR.to_vec();
        let symbol = match source.call2(address, call_data, block_number).await {
            Ok(output) => {
                String::from_utf8(output.to_vec()).ok().map(|s| remove_control_characters(&s))
            }
            Err(_) => None,
        };

        // decimals
        let call_data = ERC20::decimalsCall::SELECTOR.to_vec();
        let decimals = match source.call2(address, call_data, block_number).await {
            Ok(output) => bytes_to_u32(output).ok(),
            Err(_) => None,
        };

        Ok((request.block_number()? as u32, request.address()?, name, symbol, decimals))
    }

    fn transform(response: Self::Response, columns: &mut Self, query: &Arc<Query>) -> R<()> {
        let schema = query.schemas.get_schema(&Datatype::Erc20Metadata)?;
        let (block, address, name, symbol, decimals) = response;
        columns.n_rows += 1;
        store!(schema, columns, block_number, block);
        store!(schema, columns, erc20, address);
        store!(schema, columns, name, name);
        store!(schema, columns, symbol, symbol);
        store!(schema, columns, decimals, decimals);
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

impl CollectByTransaction for Erc20Metadata {
    type Response = ();
}

impl MulticallBatchable for Erc20Metadata {
    fn calls_for_row(params: &Params, require_success: bool) -> R<Vec<Multicall3::Call3>> {
        let target = Address::from_slice(&params.address()?);
        let allow_failure = !require_success;
        Ok(vec![
            Multicall3::Call3 {
                target,
                allowFailure: allow_failure,
                callData: Bytes::from(ERC20::nameCall::SELECTOR.to_vec()),
            },
            Multicall3::Call3 {
                target,
                allowFailure: allow_failure,
                callData: Bytes::from(ERC20::symbolCall::SELECTOR.to_vec()),
            },
            Multicall3::Call3 {
                target,
                allowFailure: allow_failure,
                callData: Bytes::from(ERC20::decimalsCall::SELECTOR.to_vec()),
            },
        ])
    }

    fn decode_row(params: &Params, results: &[Multicall3::Result]) -> R<Self::Response> {
        let name = if results[0].success {
            decode_string_or_bytes32(&results[0].returnData)
        } else {
            None
        };
        let symbol = if results[1].success {
            decode_string_or_bytes32(&results[1].returnData)
        } else {
            None
        };
        let decimals = if results[2].success && !results[2].returnData.is_empty() {
            bytes_to_u32(alloy::primitives::Bytes::copy_from_slice(&results[2].returnData)).ok()
        } else {
            None
        };
        Ok((params.block_number()? as u32, params.address()?, name, symbol, decimals))
    }
}
