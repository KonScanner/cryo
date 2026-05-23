//! Multicall3 helpers for batching `eth_call` invocations.
//!
//! [Multicall3](https://www.multicall3.com) is a thin aggregator contract that
//! exposes `aggregate3(Call3[])` — given a list of `(target, allowFailure,
//! callData)` triples it returns a parallel list of `(success, returnData)`
//! results in one RPC round-trip. The contract is CREATE2-deployed at the
//! same address (`0xcA11bde05977b3631167028862bE2a173976CA11`) on every
//! supported chain.
//!
//! Cryo uses this to collapse N `eth_call` requests into one for the
//! `eth_calls` dataset when `query.multicall` is enabled.

use alloy::{
    primitives::{address, Address},
    sol,
};

/// Canonical Multicall3 deploy address (same on every supported chain).
pub const MULTICALL3_ADDRESS: Address = address!("cA11bde05977b3631167028862bE2a173976CA11");

/// Default batch size used when the caller doesn't specify one.
///
/// Matches the upstream Python tooling default (`batch_size=150`). At ~50k gas
/// per inner call this stays comfortably under a 30M-gas block limit while
/// keeping decoded result sets a manageable size per RPC reply.
pub const DEFAULT_MULTICALL_BATCH_SIZE: u32 = 150;

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

/// Block at which Multicall3 was first deployed on a given chain.
///
/// Returns `None` for chains we have not verified — callers should treat
/// "unknown chain" as "Multicall3 unavailable" and fall back to per-call
/// extraction. Add new chains only after cross-checking the deploy block
/// from the canonical [deploys list](https://www.multicall3.com/deployments).
pub fn multicall3_deploy_block(chain_id: u64) -> Option<u64> {
    match chain_id {
        1 => Some(14_353_601),      // Ethereum mainnet
        10 => Some(4_286_263),      // Optimism
        56 => Some(15_921_452),     // BNB Chain
        100 => Some(21_022_491),    // Gnosis
        137 => Some(25_770_160),    // Polygon
        8_453 => Some(5_022),       // Base
        42_161 => Some(7_654_707),  // Arbitrum One
        43_114 => Some(11_907_934), // Avalanche C-Chain
        _ => None,
    }
}
