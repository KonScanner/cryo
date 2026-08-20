use crate::types::Datatype;

/// enum of possible sets of datatypes that triodion can collect
/// used when multiple datatypes are collected together
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize)]
pub enum MultiDatatype {
    /// blocks and transactions
    BlocksAndTransactions,

    /// call trace derivatives
    CallTraceDerivatives,

    /// geth debug versions of balance diffs, code diffs, nonce diffs, and storage diffs
    GethStateDiffs,

    /// Log-derived datasets coalesced through one `eth_getLogs` per partition.
    /// Members include raw `logs`, `erc20_transfers`, `erc20_approvals`,
    /// `erc721_transfers`, and `erc20_wrapper_events` (Deposit/Withdrawal). All
    /// share the `eth_getLogs` RPC primitive; when two or more are co-requested
    /// triodion issues **one** call per partition with a UNION filter and fans the
    /// response out to each requested sub-dataset's column accumulator.
    LogEvents,

    /// balance diffs, code diffs, nonce diffs, and storage diffs
    StateDiffs,

    /// balance reads, code reads, nonce reads, and storage reads
    StateReads,
}

impl MultiDatatype {
    /// individual datatypes
    pub fn datatypes(&self) -> Vec<Datatype> {
        match &self {
            MultiDatatype::BlocksAndTransactions => vec![Datatype::Blocks, Datatype::Transactions],
            MultiDatatype::CallTraceDerivatives => {
                vec![Datatype::Contracts, Datatype::NativeTransfers, Datatype::Traces]
            }
            MultiDatatype::GethStateDiffs => vec![
                Datatype::GethBalanceDiffs,
                Datatype::GethCodeDiffs,
                Datatype::GethNonceDiffs,
                Datatype::GethStorageDiffs,
            ],
            MultiDatatype::LogEvents => vec![
                Datatype::Logs,
                Datatype::Erc20Transfers,
                Datatype::Erc20Approvals,
                Datatype::Erc721Transfers,
                Datatype::Erc20WrapperEvents,
            ],
            MultiDatatype::StateDiffs => vec![
                Datatype::BalanceDiffs,
                Datatype::CodeDiffs,
                Datatype::NonceDiffs,
                Datatype::StorageDiffs,
            ],
            MultiDatatype::StateReads => vec![
                Datatype::BalanceReads,
                Datatype::CodeReads,
                Datatype::NonceReads,
                Datatype::StorageReads,
            ],
        }
    }

    /// return all variants of multi datatype
    pub fn variants() -> Vec<MultiDatatype> {
        vec![
            MultiDatatype::BlocksAndTransactions,
            MultiDatatype::CallTraceDerivatives,
            MultiDatatype::GethStateDiffs,
            MultiDatatype::LogEvents,
            MultiDatatype::StateDiffs,
            MultiDatatype::StateReads,
        ]
    }

    /// name
    pub fn name(&self) -> String {
        format!("{}", heck::AsSnakeCase(format!("{:?}", self)))
    }
}
