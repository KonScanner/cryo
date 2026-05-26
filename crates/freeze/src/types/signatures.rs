use alloy::sol;

sol! {
    contract ERC20 {
        function name() external pure returns (string memory);
        function symbol() external pure returns (string memory);
        function decimals() external pure returns (uint8);
        function totalSupply() external view returns (uint);
        function balanceOf(address owner) external view returns (uint);
        function allowance(address owner, address spender) external view returns (uint);

        event Approval(address indexed owner, address indexed spender, uint value);
        event Transfer(address indexed from, address indexed to, uint value);
    }
}

sol! {
    contract ERC721 {
        event Transfer(address indexed from, address indexed to, uint256 indexed tokenId);
        event Approval(address indexed owner, address indexed approved, uint256 indexed tokenId);
        event ApprovalForAll(address indexed owner, address indexed operator, bool approved);
        function name() external view returns (string memory);
        function symbol() external view returns (string memory);
        function tokenURI(uint256 tokenId) external view returns (string memory);
    }
}

sol! {
    /// Supply-modifying events that ERC-20 wrappers and many DeFi tokens emit
    /// **in addition** to the standard ERC-20 Transfer (which already covers
    /// mints/burns via the zero-address sender/receiver convention). Captured
    /// at the event-signature level — any contract emitting these signatures
    /// is indexed regardless of whether it's a "real" WETH-style wrapper.
    contract ERC20Wrapper {
        /// WETH-style mint: depositing the underlying (ETH for WETH) returns
        /// the wrapped token. `wad` is the amount minted.
        event Deposit(address indexed dst, uint256 wad);

        /// WETH-style burn: redeeming the wrapped token for the underlying.
        /// `wad` is the amount burned.
        event Withdrawal(address indexed src, uint256 wad);
    }
}
