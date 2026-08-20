use crate::*;
use alloy::{
    primitives::U256,
    rpc::types::{Filter, Log, Topic},
    sol_types::SolEvent,
};
use polars::prelude::*;

/// columns for transactions
#[triodion_macros::to_df(Datatype::Erc20Transfers)]
#[derive(Default)]
pub struct Erc20Transfers {
    n_rows: u64,
    block_number: Vec<u32>,
    block_hash: Vec<Option<Vec<u8>>>,
    transaction_index: Vec<u32>,
    log_index: Vec<u32>,
    transaction_hash: Vec<Vec<u8>>,
    erc20: Vec<Vec<u8>>,
    from_address: Vec<Vec<u8>>,
    to_address: Vec<Vec<u8>>,
    value: Vec<U256>,
    chain_id: Vec<u64>,
}

impl Dataset for Erc20Transfers {
    fn default_columns() -> Option<Vec<&'static str>> {
        Some(vec![
            "block_number",
            // "block_hash",
            "transaction_index",
            "log_index",
            "transaction_hash",
            "erc20",
            "from_address",
            "to_address",
            "value",
            "chain_id",
        ])
    }

    fn optional_parameters() -> Vec<Dim> {
        vec![Dim::Address, Dim::Topic0, Dim::Topic1, Dim::Topic2, Dim::FromAddress, Dim::ToAddress]
    }

    fn use_block_ranges() -> bool {
        true
    }

    fn default_inner_request_size() -> u64 {
        // Log fetch is one HTTP request per block range; pulling 50 blocks per
        // request is a safe default for ERC-20 Transfer-shaped filters on a
        // single contract. Users can override with --inner-request-size.
        50
    }

    fn arg_aliases() -> Option<std::collections::HashMap<Dim, Dim>> {
        Some([(Dim::Contract, Dim::Address)].into_iter().collect())
    }
}

impl CollectByBlock for Erc20Transfers {
    type Response = Vec<Log>;

    async fn extract(request: Params, source: Arc<Source>, _: Arc<Query>) -> R<Self::Response> {
        let mut topics: [Topic; 4] = Default::default();
        topics[0] = ERC20::Transfer::SIGNATURE_HASH.into();
        if let Some(from_address) = &request.from_address {
            // `--from-address` is documented as a 20-byte address; left-pad it into
            // the 32-byte topic slot rather than panicking in `B256::from_slice`.
            let v = address_dim_as_topic(from_address).ok_or_else(|| {
                CollectError::CollectError("from_address must be at most 32 bytes".to_string())
            })?;
            topics[1] = v.into();
        }
        if let Some(to_address) = &request.to_address {
            let v = address_dim_as_topic(to_address).ok_or_else(|| {
                CollectError::CollectError("to_address must be at most 32 bytes".to_string())
            })?;
            topics[2] = v.into();
        }
        let filter = Filter { topics, ..request.ethers_log_filter()? };
        let logs = source.get_logs(&filter).await?;

        Ok(logs
            .into_iter()
            .filter(|x| x.topics().len() == 3 && x.data().data.len() == 32)
            .collect())
    }

    fn transform(response: Self::Response, columns: &mut Self, query: &Arc<Query>) -> R<()> {
        let schema = query.schemas.get_schema(&Datatype::Erc20Transfers)?;
        process_erc20_transfers(response, columns, schema)
    }
}

impl CollectByTransaction for Erc20Transfers {
    type Response = Vec<Log>;

    async fn extract(request: Params, source: Arc<Source>, _: Arc<Query>) -> R<Self::Response> {
        let logs = source.get_transaction_logs(request.transaction_hash()?).await?;
        Ok(logs.into_iter().filter(is_erc20_transfer).collect())
    }

    fn transform(response: Self::Response, columns: &mut Self, query: &Arc<Query>) -> R<()> {
        let schema = query.schemas.get_schema(&Datatype::Erc20Transfers)?;
        process_erc20_transfers(response, columns, schema)
    }
}

/// True iff `log` has the ERC-20 `Transfer` shape: Transfer signature, 3 topics
/// (sig + indexed from + indexed to), and a 32-byte value. Shared with the
/// coalesced [`crate::LogEvents`] extractor — both `fan_out_block` and
/// `fan_out_transaction`.
pub(crate) fn is_erc20_transfer(log: &Log) -> bool {
    log.topics().len() == 3 &&
        log.data().data.len() == 32 &&
        log.topics().first().is_some_and(|t| *t == ERC20::Transfer::SIGNATURE_HASH)
}

/// process block into columns
fn process_erc20_transfers(logs: Vec<Log>, columns: &mut Erc20Transfers, schema: &Table) -> R<()> {
    for log in logs.iter() {
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
            store!(schema, columns, from_address, log.topics()[1][12..].to_vec());
            store!(schema, columns, to_address, log.topics()[2][12..].to_vec());
            store!(schema, columns, value, U256::from_be_slice(&log.data().data));
        }
    }
    Ok(())
}
