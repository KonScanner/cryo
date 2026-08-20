# triodion

[![Rust](https://github.com/KonScanner/triodion/actions/workflows/build_and_test.yml/badge.svg)](https://github.com/KonScanner/triodion/actions/workflows/build_and_test.yml)

> **triodion** is a modified fork of [cryo](https://github.com/paradigmxyz/cryo), originally
> developed by Paradigm and the Cryo Contributors. It is not affiliated with, nor endorsed
> by, Paradigm. It diverges from upstream commit `559b654` (2025-01-08) and was renamed
> from `cryo` to `triodion` to avoid namespace collision. See [NOTICE](NOTICE) for the
> statement of changes required by Apache-2.0 section 4(b). Licensed MIT OR Apache-2.0.

`triodion` is the easiest way to extract blockchain data to parquet, csv, json, or a python dataframe.

`triodion` is also extremely flexible, with [many different options](#triodion-help) to control how data is extracted + filtered + formatted

*`triodion` is an early WIP, please report bugs + feedback to the issue tracker*

*note that `triodion`'s default settings will slam a node too hard for use with 3rd party RPC providers. Instead, `--requests-per-second` and `--max-concurrent-requests` should be used to impose ratelimits. Such settings will be handled automatically in a future release*.

## Contents

1. [Example Usage](#example-usage)
2. [Installation](#installation)
3. [Data Schema](#data-schemas)
4. [Code Guide](#code-guide)
5. [Documentation](#documentation)
    1. [Basics](#triodion-help)
    2. [Syntax](#triodion-syntax)
    3. [Datasets](#triodion-datasets)

## Example Usage

use as `triodion <dataset> [OPTIONS]`

| Example | Command |
| :- | :- |
| Extract all logs from block 16,000,000 to block 17,000,000 | `triodion logs -b 16M:17M` |
| Extract blocks, logs, or traces missing from current directory | `triodion blocks txs traces` |
| Extract to csv instead of parquet | `triodion blocks txs traces --csv` |
| Extract only certain columns | `triodion blocks --include number timestamp` |
| Dry run to view output schemas or expected work | `triodion storage_diffs --dry` |
| Extract all USDC events | `triodion logs --contract 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48` |

For a more complex example, see the [Uniswap Example](./examples/uniswap.sh).

`triodion` uses `ETH_RPC_URL` env var as the data source unless `--rpc <url>` is given

## Installation

The simplest way to use `triodion` is as a cli tool:

#### Method 1: install from source

```bash
git clone https://github.com/KonScanner/triodion
cd triodion
cargo install --path ./crates/cli
```

This method requires having rust installed. See [rustup](https://rustup.rs/) for instructions.

#### Method 2: install from git

```bash
cargo install --git https://github.com/KonScanner/triodion triodion-cli
```

The `triodion-*` crates are not published to crates.io. Install from git or from source.

This method requires having rust installed. See [rustup](https://rustup.rs/) for instructions.

Make sure that `~/.cargo/bin` is on your `PATH`. One way to do this is by adding the line `export PATH="$HOME/.cargo/bin:$PATH"` to your `~/.bashrc` or `~/.profile`.

### Python Installation

`triodion` can also be installed as a python package:

#### Installing the python package from pypi

(make sure rust is installed first, see [rustup](https://www.rust-lang.org/tools/install))

```bash
pip install maturin
pip install triodion
```

```python
import triodion
```

#### Installing the python package from source

```bash
pip install maturin
git clone https://github.com/KonScanner/triodion
cd triodion/crates/python
maturin build --release
pip install --force-reinstall <OUTPUT_OF_MATURIN_BUILD>.whl
```

## Data Schemas

Many `triodion` cli options will affect output schemas by adding/removing columns or changing column datatypes.

`triodion` will always print out data schemas before collecting any data. To view these schemas without collecting data, use `--dry` to perform a dry run.

#### Schema Design Guide

An attempt is made to ensure that the dataset schemas conform to a common set of design guidelines:
- By default, rows should contain enough information in their columns to be order-able (unless the rows do not have an intrinsic order).
- Columns should usually be named by their JSON-RPC or ethers.rs defaults, except in cases where a much more explicit name is available.
- To make joins across tables easier, a given piece of information should use the same datatype and column name across tables when possible.
- Large ints such as `u256` should allow multiple conversions. A `value` column of type `u256` should allow: `value_binary`, `value_string`, `value_f32`, `value_f64`, `value_u32`, `value_u64`, and `value_d128`. These types can be specified at runtime using the `--u256-types` argument.
- By default, columns related to non-identifying cryptographic signatures are omitted by default. For example, `state_root` of a block or `v`/`r`/`s` of a transaction.
- Integer values that can never be negative should be stored as unsigned integers.
- Every table should allow a `chain_id` column so that data from multiple chains can be easily stored in the same table.

Standard types across tables:
- `block_number`: `u32`
- `transaction_index`: `u32`
- `nonce`: `u32`
- `gas_used`: `u64`
- `gas_limit`: `u64`
- `chain_id`: `u64`
- `timestamp`: `u32`

#### JSON-RPC

`triodion` currently obtains all of its data using the [JSON-RPC](https://ethereum.org/en/developers/docs/apis/json-rpc/) protocol standard.

|dataset|blocks per request|results per block|method|
|-|-|-|-|
|Blocks|1|1|`eth_getBlockByNumber`|
|Transactions|1|multiple|`eth_getBlockByNumber`, `eth_getBlockReceipts`, `eth_getTransactionReceipt`|
|Logs|multiple|multiple|`eth_getLogs`|
|Contracts|1|multiple|`trace_block`|
|Traces|1|multiple|`trace_block`|
|State Diffs|1|multiple|`trace_replayBlockTransactions`|
|Vm Traces|1|multiple|`trace_replayBlockTransactions`|

`triodion` uses [alloy](https://github.com/alloy-rs/alloy) to perform JSON-RPC requests, so it can be used with any chain that alloy is compatible with. This includes Ethereum, Optimism, Arbitrum, Polygon, BNB, and Avalanche.

A future version of `triodion` will be able to bypass JSON-RPC and query node data directly.

## Code Guide
- Code is arranged into the following crates:
    - `triodion-cli`: convert textual data into triodion function calls
    - `triodion-core`: core triodion code
    - `triodion-py`: python adapter (imported as `triodion`, distributed as `triodion`)
    - `triodion-macros`: procedural macro for generating dataset definitions
- Do not use panics (including `panic!`, `todo!`, `unwrap()`, and `expect()`) except in the following circumstances: tests, build scripts, lazy static blocks, and procedural macros

## Documentation

1. [triodion help](#triodion-help)
2. [triodion syntax](#triodion-syntax)
3. [triodion datasets](#triodion-datasets)

#### triodion help

(output of `triodion help`)

```
triodion extracts blockchain data to parquet, csv, or json

Usage: triodion [OPTIONS] [DATATYPE]...

Arguments:
  [DATATYPE]...  datatype(s) to collect, use triodion datasets to see all available

Options:
      --remember    Remember current command for future use
  -v, --verbose     Extra verbosity
      --no-verbose  Run quietly without printing information to stdout
  -h, --help        Print help (see more with '--help')
  -V, --version     Print version

Content Options:
  -b, --blocks <BLOCKS>...            Block numbers, see syntax below
      --timestamps [<TIMESTAMPS>...]  Timestamps in unix, see syntax below
  -t, --txs <TXS>...                  Transaction hashes, see syntax below
  -a, --align                         Align chunk boundaries to regular intervals,
                                      e.g. (1000 2000 3000), not (1106 2106 3106)
      --reorg-buffer <N_BLOCKS>       Reorg buffer, save blocks only when this old,
                                      can be a number of blocks [default: 0]
  -i, --include-columns [<COLS>...]   Columns to include alongside the defaults,
                                      use `all` to include all available columns
  -e, --exclude-columns [<COLS>...]   Columns to exclude from the defaults
      --columns [<COLS>...]           Columns to use instead of the defaults,
                                      use `all` to use all available columns
      --u256-types <U256_TYPES>...    Set output datatype(s) of U256 integers
                                      [default: binary, string, f64]
      --hex                           Use hex string encoding for binary columns
  -s, --sort [<SORT>...]              Columns(s) to sort by, `none` for unordered
      --exclude-failed                Exclude items from failed transactions

Source Options:
  -r, --rpc <RPC>                    RPC url [default: 1. MESC 2. ETH_RPC_URL]
      --l1-rpc <URL>                 L1 (settlement) RPC url for L2 datasets that read L1-side events
      --network-name <NETWORK_NAME>  Network name [default: name of eth_getChainId]

Acquisition Options:
  -l, --requests-per-second <limit>   Ratelimit on requests per second
      --max-retries <R>               Max retries for provider errors [default: 5]
      --initial-backoff <B>           Initial retry backoff time (ms) [default: 500]
      --compute-units-per-second <U>  The number of compute units per second for this provider [default: 50]
      --max-concurrent-requests <M>   Global number of concurrent requests
      --max-concurrent-chunks <M>     Number of chunks processed concurrently
      --chunk-order <CHUNK_ORDER>     Chunk collection order (normal, reverse, random)
  -d, --dry                           Dry run, collect no data

Output Options:
  -c, --chunk-size <CHUNK_SIZE>      Number of blocks per file [default: 1000]
      --n-chunks <N_CHUNKS>          Number of files (alternative to --chunk-size)
      --partition-by <PARTITION_BY>  Dimensions to partition by
  -o, --output-dir <OUTPUT_DIR>      Directory for output files [default: .]
      --subdirs <SUBDIRS>...         Subdirectories for output files
                                     can be `datatype`, `network`, or custom string
      --label <LABEL>                Label to add to each filename
      --overwrite                    Overwrite existing files instead of skipping
      --csv                          Save as csv instead of parquet
      --json                         Save as json instead of parquet
      --row-group-size <GROUP_SIZE>  Number of rows per row group in parquet file
      --n-row-groups <N_ROW_GROUPS>  Number of rows groups in parquet file
      --no-stats                     Do not write statistics to parquet files
      --compression <NAME [#]>...    Compression algorithm and level [default: lz4]
      --report-dir <REPORT_DIR>      Directory to save summary report
                                     [default: {output_dir}/.triodion/reports]
      --no-report                    Avoid saving a summary report

Dataset-specific Options:
      --address <ADDRESS>...         Address(es)
      --to-address <address>...      To Address(es)
      --from-address <address>...    From Address(es)
      --call-data <CALL_DATA>...     Call data(s) to use for eth_calls
      --function <FUNCTION>...       Function(s) to use for eth_calls
      --inputs <INPUTS>...           Input(s) to use for eth_calls
      --slot <SLOT>...               Slot(s)
      --contract <CONTRACT>...       Contract address(es)
      --topic0 <TOPIC0>...           Topic0(s) [aliases: event]
      --topic1 <TOPIC1>...           Topic1(s)
      --topic2 <TOPIC2>...           Topic2(s)
      --topic3 <TOPIC3>...           Topic3(s)
      --event-signature <SIG>...     Event signature for log decoding
      --inner-request-size <BLOCKS>  Blocks per request (eth_getLogs) [default: 1]
      --js-tracer <tracer>           Event signature for log decoding
      --no-multicall                 Disable Multicall3 batching for eth_calls / erc20_balances
      --multicall-batch-size <N>     Cap on inner eth_calls per Multicall3 batch (0 = the dataset's own default) [default: 0]
      --multicall-require-success    Mark the whole batch as failed if any inner call reverts (default: per-call failures return null)

Optional Subcommands:
      triodion help                      display help message
      triodion help syntax               display block + tx specification syntax
      triodion help datasets             display list of all datasets
      triodion help <DATASET(S)>         display info about a dataset
```

#### triodion syntax

(output of `triodion help syntax`)

```
Block specification syntax
- can use numbers                    --blocks 5000 6000 7000
- can use ranges                     --blocks 12M:13M 15M:16M
- can use a parquet file             --blocks ./path/to/file.parquet[:COLUMN_NAME]
- can use multiple parquet files     --blocks ./path/to/files/*.parquet[:COLUMN_NAME]
- numbers can contain { _ . K M B }  5_000 5K 15M 15.5M
- omitting range end means latest    15.5M: == 15.5M:latest
- omitting range start means 0       :700 == 0:700
- minus on start means minus end     -1000:7000 == 6000:7000
- plus sign on end means plus start  15M:+1000 == 15M:15.001K
- can use every nth value            2000:5000:1000 == 2000 3000 4000
- can use n values total             100:200/5 == 100 124 149 174 199

Transaction specification syntax
- can use transaction hashes         --txs TX_HASH1 TX_HASH2 TX_HASH3
- can use a parquet file             --txs ./path/to/file.parquet[:COLUMN_NAME]
                                     (default column name is transaction_hash)
- can use multiple parquet files     --txs ./path/to/ethereum__logs*.parquet
```

#### triodion datasets

(output of `triodion help datasets`)

```
triodion datasets
─────────────────
- address_appearances
- balance_diffs
- balance_reads
- balances
- blocks
- code_diffs
- code_reads
- codes
- contracts
- erc20_balances
- erc20_metadata
- erc20_supplies
- erc20_transfers
- erc20_approvals
- erc20_wrapper_events (aliases = wrapper_events, weth_events)
- erc721_metadata
- erc721_transfers
- eth_calls
- four_byte_counts (alias = 4byte_counts)
- geth_calls
- geth_code_diffs
- geth_balance_diffs
- geth_storage_diffs
- geth_nonce_diffs
- geth_opcodes
- javascript_traces (alias = js_traces)
- logs (alias = events)
- native_transfers
- nonce_diffs
- nonce_reads
- nonces
- slots (alias = storages)
- storage_diffs (alias = slot_diffs)
- storage_reads (alias = slot_reads)
- traces
- trace_calls
- transactions (alias = txs)
- vm_traces (alias = opcode_traces)

dataset group names
───────────────────
- blocks_and_transactions: blocks, transactions
- call_trace_derivatives: contracts, native_transfers, traces
- geth_state_diffs: geth_balance_diffs, geth_code_diffs, geth_nonce_diffs, geth_storage_diffs
- log_events: logs, erc20_transfers, erc20_approvals, erc721_transfers, erc20_wrapper_events
- state_diffs: balance_diffs, code_diffs, nonce_diffs, storage_diffs
- state_reads: balance_reads, code_reads, nonce_reads, storage_reads

use triodion help <DATASET> to print info about a specific dataset
```
