
## Installation

There are two main options for installing `triodion`.

Installing from pip is faster and does not require rust to be installed.

Installing from source allows using the latest unreleased version of triodion.

#### Option 1: Install from pip

```
pip install triodion
```

#### Option 2: Install from source

```
pip install maturin
git clone https://github.com/KonScanner/triodion
cd triodion/crates/python
maturin build --release
pip install --force-reinstall <OUTPUT_OF_MATURIN_BUILD>.whl
```

#### Other notes

If you would like `triodion` to output results using pandas instead of polars, also install pandas: `pip install pandas`

