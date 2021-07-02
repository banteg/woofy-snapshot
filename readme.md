# Woofy Snapshot for Project Galaxy

Snapshots balances across multiple chains, unwraps balances inside AMMs.

## Usage

1. Run the script for each network.

*NOTE:* Archive node is required to Ethereum snapshot to unwrap balances inside Uniswap V3.

```
brownie run snapshot --network mainnet
brownie run snapshot --network ftm-main
brownie run snapshot --network bsc-main
```

2. Combine balances into chances.

```
brownie run snapshot combine --network mainnet
```
