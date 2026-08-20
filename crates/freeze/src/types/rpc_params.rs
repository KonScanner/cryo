use crate::{err, CollectError};
use alloy::{
    primitives::{Address, BlockNumber, B256},
    rpc::types::{Filter, FilterBlockOption},
};

/// represents parameters for a single rpc call
#[derive(Default, Clone, Debug)]
pub struct Params {
    /// block number
    pub block_number: Option<u64>,
    /// block range
    pub block_range: Option<(u64, u64)>,
    /// transaction
    pub transaction_hash: Option<Vec<u8>>,
    /// call data
    pub call_data: Option<Vec<u8>>,
    /// address
    pub address: Option<Vec<u8>>,
    /// contract
    pub contract: Option<Vec<u8>>,
    /// from address
    pub from_address: Option<Vec<u8>>,
    /// to address
    pub to_address: Option<Vec<u8>>,
    /// slot
    pub slot: Option<Vec<u8>>,
    /// topic0
    pub topic0: Option<Vec<u8>>,
    /// topic1
    pub topic1: Option<Vec<u8>>,
    /// topic2
    pub topic2: Option<Vec<u8>>,
    /// topic3
    pub topic3: Option<Vec<u8>>,
}

impl Params {
    /// block number
    pub fn block_number(&self) -> Result<u64, CollectError> {
        self.block_number.ok_or(err("block_number not specified"))
    }

    /// block range
    pub fn block_range(&self) -> Result<(u64, u64), CollectError> {
        self.block_range.ok_or(err("block_range not specified"))
    }

    /// transaction
    pub fn transaction_hash(&self) -> Result<Vec<u8>, CollectError> {
        self.transaction_hash.clone().ok_or(err("transaction not specified"))
    }

    /// address
    pub fn address(&self) -> Result<Vec<u8>, CollectError> {
        self.address.clone().ok_or(err("address not specified"))
    }

    /// contract
    pub fn contract(&self) -> Result<Vec<u8>, CollectError> {
        self.contract.clone().ok_or(err("contract not specified"))
    }

    /// slot
    pub fn slot(&self) -> Result<Vec<u8>, CollectError> {
        self.slot.clone().ok_or(err("slot not specified"))
    }

    /// call_data
    pub fn call_data(&self) -> Result<Vec<u8>, CollectError> {
        self.call_data.clone().ok_or(err("call_data not specified"))
    }

    //
    // ethers versions
    //

    /// ethers block number
    pub fn ethers_block_number(&self) -> Result<BlockNumber, CollectError> {
        self.block_number()
    }

    /// ethers transaction
    ///
    /// # Errors
    /// Errors if `transaction_hash` is unset, or is not exactly 32 bytes.
    pub fn ethers_transaction_hash(&self) -> Result<B256, CollectError> {
        fixed_from_slice(&self.transaction_hash()?, "transaction_hash")
    }

    /// ethers address
    ///
    /// # Errors
    /// Errors if `address` is unset, or is not exactly 20 bytes.
    pub fn ethers_address(&self) -> Result<Address, CollectError> {
        fixed_from_slice(&self.address()?, "address")
    }

    /// ethers contract
    ///
    /// # Errors
    /// Errors if `contract` is unset, or is not exactly 20 bytes.
    pub fn ethers_contract(&self) -> Result<Address, CollectError> {
        fixed_from_slice(&self.contract()?, "contract")
    }

    /// log filter
    ///
    /// # Errors
    /// Errors if the block range is unset, or if `address` / `topic0..3` are
    /// present but not the exact byte width of their type. These bytes come
    /// straight from the CLI (`--address`, `--topic0..3`), which hex-decodes
    /// without a length check, so the width is validated here.
    pub fn ethers_log_filter(&self) -> Result<Filter, CollectError> {
        let (start, end) = self.block_range()?;
        let block_option =
            FilterBlockOption::Range { from_block: Some(start.into()), to_block: Some(end.into()) };
        let mut filter = Filter::new();
        filter.block_option = block_option;
        if let Some(address) = &self.address {
            filter = filter.address(fixed_from_slice::<Address>(address, "address")?);
        }
        if let Some(topic0) = &self.topic0 {
            filter = filter.event_signature(fixed_from_slice::<B256>(topic0, "topic0")?);
        }
        if let Some(topic1) = &self.topic1 {
            filter = filter.topic1(fixed_from_slice::<B256>(topic1, "topic1")?);
        }
        if let Some(topic2) = &self.topic2 {
            filter = filter.topic2(fixed_from_slice::<B256>(topic2, "topic2")?);
        }
        if let Some(topic3) = &self.topic3 {
            filter = filter.topic3(fixed_from_slice::<B256>(topic3, "topic3")?);
        }
        Ok(filter)
    }
}

/// Convert user-supplied bytes into a fixed-width alloy type, or error.
///
/// alloy's `Address::from_slice` / `B256::from_slice` **panic** on a length
/// mismatch, and these bytes arrive straight from the CLI: `parse_binary_arg`
/// hex-decodes `--address` / `--topic0..3` with no length check, so
/// `--topic1 0xdead` reaches here as 2 bytes. A malformed argument must be a
/// clean error, not an abort mid-crawl.
///
/// # Errors
/// Errors if `bytes` is not exactly the byte width of `T`.
pub(crate) fn fixed_from_slice<'a, T>(bytes: &'a [u8], name: &str) -> Result<T, CollectError>
where
    T: TryFrom<&'a [u8]>,
{
    T::try_from(bytes).map_err(|_| {
        err(&format!("{} must be {} bytes, got {}", name, std::mem::size_of::<T>(), bytes.len()))
    })
}
