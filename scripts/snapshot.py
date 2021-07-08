import json
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from fractions import Fraction
from glob import glob
from itertools import count
from brownie.utils.output import build_tree

from brownie import ZERO_ADDRESS, Contract, chain, interface, web3
from camera_shy import uniswap_v3, masterchef
from camera_shy.common import (
    block_after_timestamp,
    decode_logs,
    get_code,
    get_token_transfers,
    merge_balances,
    transfers_to_balances,
    unwrap_balances,
    filter_contracts,
    eth_call,
)
from click import secho, style
from toolz import concat, groupby, valmap, unique
from tqdm import tqdm

SNAPSHOT_START = datetime(2021, 5, 12, tzinfo=timezone.utc)
SNAPSHOT_INTERVAL = timedelta(days=7)
MIN_BALANCE = 2000 * 10 ** 12
CHAINS = {
    1: {
        "network": "eth",
        "woofy": "0xD0660cD418a64a1d44E9214ad8e459324D8157f1",
        "deploy_block": 12414993,
    },
    250: {
        "network": "ftm",
        "woofy": "0xD0660cD418a64a1d44E9214ad8e459324D8157f1",
        "vault": "0x6fCE944d1f2f877B3972e0E8ba81d27614D62BeD",
        "deploy_block": 6146773,
    },
    56: {
        "network": "bsc",
        "woofy": "0xD0660cD418a64a1d44E9214ad8e459324D8157f1",
        "deploy_block": 7363975,
    },
    137: {
        "network": "matic",
        "woofy": "0xD0660cD418a64a1d44E9214ad8e459324D8157f1",
        "vault": "0xEAFB3Ee25B5a9a1b35F193A4662E3bDba7A95BEb",
        "deploy_block": 14604154,
    },
}
UNISWAP_V3_FACTORY = "0x1F98431c8aD98523631AE4a59f267346ea31F984"
CHAIN = CHAINS[chain.id]
WOOFY = CHAIN["woofy"]
VAULT = CHAIN.get("vault")
DEPLOY_BLOCK = CHAIN["deploy_block"]


def generate_snapshot_blocks(start, interval):
    """
    Generate snapshot block numbers at a certain interval.
    """
    epochs = {}
    for period in count():
        timestamp = start + interval * period
        if timestamp > datetime.now(tz=timezone.utc):
            break

        block = block_after_timestamp(timestamp)
        print(f"{timestamp} -> {block}")
        epochs[str(timestamp)] = block

    return epochs


def unwrap_uniswap_v3(snapshot, block):
    secho("Fetch Uniswap v3 Positions", fg="yellow")
    uniswap_v3_positions = uniswap_v3.fetch_uniswap_v3_positions(block)

    secho(f"Looking for Uniswap v3 Pools", fg="yellow")
    uniswap_pools = [
        user for user in tqdm(snapshot) if uniswap_v3.is_uniswap_v3_pool(user)
    ]
    secho(f"Found {len(uniswap_pools)} Uniswap v3 Pools", fg="yellow")

    replacements = {}
    for pool in uniswap_pools:
        replacements[pool] = uniswap_v3.unwrap_liquidity(
            interface.IUniswapV3Pool(pool),
            Contract(WOOFY),
            uniswap_v3_positions,
            block,
            MIN_BALANCE,
        )
    return replacements


def unwrap_lp_tokens(snapshot, block, min_balance=0):
    contracts = filter_contracts(snapshot)
    replacements = {}
    pools = []

    for pool in tqdm(contracts, desc="identify pools"):
        try:
            reserves = eth_call(pool, "getReserves()(uint112,uint112,uint32)")
            factory = eth_call(pool, "factory()(address)")
        except ValueError:
            continue
        else:
            # cache the abi to skip pulling from explorer
            contract = interface.IUniswapV2Pair(pool)
            pools.append(pool)

    if pools:
        print(
            build_tree([[style("Uniswap V2 like pools", fg="bright_magenta"), *pools]])
        )
    else:
        secho("No Uniswap V2 like pools found", fg="bright_magenta")

    for pool in pools:
        logs = get_token_transfers(pool, DEPLOY_BLOCK)
        events = decode_logs(list(logs))
        balances = transfers_to_balances(events, block)
        supply = sum(balances.values())
        if not supply:
            continue
        replacements[pool] = {
            user: int(Fraction(balances[user], supply) * snapshot[pool])
            for user in balances
        }
        replacements[pool] = {
            user: balance
            for user, balance in replacements[pool].items()
            if balance >= min_balance
        }

    return replacements


def main():
    epochs = generate_snapshot_blocks(SNAPSHOT_START, SNAPSHOT_INTERVAL)

    secho("Fetch Transfer logs", fg="yellow")
    logs = get_token_transfers(WOOFY, DEPLOY_BLOCK)
    events = decode_logs(list(logs))

    secho("Photograph balances at each snapshot block", fg="yellow")
    snapshots = {
        epoch: transfers_to_balances(events, block, MIN_BALANCE)
        for epoch, block in epochs.items()
    }

    if VAULT:
        secho("Check addresses for yvWoofy holders", fg="yellow")
        logs = get_token_transfers(VAULT, DEPLOY_BLOCK)
        events = decode_logs(list(logs))
        secho("Photograph balances at each snapshot block", fg="yellow")
        vault_snapshots = {
            epoch: transfers_to_balances(events, block, MIN_BALANCE)
            for epoch, block in epochs.items()
        }
        snapshots = {
            epoch: merge_balances(snapshots[epoch], vault_snapshots[epoch])
            for epoch in snapshots
        }

    secho("Check addresses for being LP contracts", fg="yellow")

    for epoch, block in epochs.items():
        replacements = {}
        replacements.update(unwrap_lp_tokens(snapshots[epoch], block, MIN_BALANCE))

        if chain.id == 1:
            replacements.update(unwrap_uniswap_v3(snapshots[epoch], block))

        snapshots[epoch] = unwrap_balances(snapshots[epoch], replacements)
        print(epoch, "after", len(snapshots[epoch]))

    unique_addresses = list(unique(concat(snapshots.values())))
    print(len(unique_addresses), "unique")
    contracts = filter_contracts(unique_addresses)
    print(len(contracts), "contracts")

    for contract in contracts:
        is_chef = masterchef.is_masterchef(contract)
        secho(f"{contract} {is_chef}", fg="green" if is_chef else "red")

    with open(f"snapshots/01-balances-{chain.id}.json", "wt") as f:
        json.dump(snapshots, f, indent=2)


def combine():
    combined_balances = defaultdict(Counter)

    # balances from all networks are combined
    sources = [json.load(open(f)) for f in glob("snapshots/01-*.json")]
    for source in sources:
        for epoch in source:
            for user, balance in source[epoch].items():
                combined_balances[epoch][user] += balance

    # each epoch where you had at least min balance adds a single chance
    chances = Counter()
    for epoch in combined_balances:
        for user in combined_balances[epoch]:
            assert combined_balances[epoch][user] >= MIN_BALANCE
            chances[user] += 1

    with open(f"snapshots/02-chances.json", "wt") as f:
        json.dump(dict(chances.most_common()), f, indent=2)

    secho("chances distributions", fg="yellow")
    for a, b in sorted(valmap(len, groupby(chances.get, chances)).items()):
        print(f"{a} {b}")

    secho("unique users", fg="yellow")
    print(len(chances))
