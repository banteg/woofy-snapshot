import json
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from fractions import Fraction
from itertools import count, zip_longest
from os import replace

import toml
from brownie import ZERO_ADDRESS, Contract, chain, interface, web3
from brownie.network.event import _decode_logs
from camera_shy import uniswap_v3
from camera_shy.common import (block_after_timestamp, decode_logs, get_code,
                               get_logs, get_token_transfers,
                               transfers_to_balances, unwrap_balances)
from click import secho
from joblib import Memory, Parallel, delayed
from tabulate import tabulate
from toolz import concat, groupby, valmap
from tqdm import tqdm, trange
from web3.middleware.filter import block_ranges

SNAPSHOT_START = datetime(2021, 5, 12, tzinfo=timezone.utc)
SNAPSHOT_INTERVAL = timedelta(days=7)
MIN_BALANCE = 2000 * 10 ** 12
CHAINS = {
    1: {
        "network": "eth",
        "woofy": "0xD0660cD418a64a1d44E9214ad8e459324D8157f1",
        "deploy_block": 12414993,
        "block_time": 13.4,
    },
    250: {
        "network": "ftm",
        "woofy": "0xD0660cD418a64a1d44E9214ad8e459324D8157f1",
        "deploy_block": 6146773,
        "block_time": 0.873,
    },
    56: {
        "network": "bsc",
        "woofy": "0xD0660cD418a64a1d44E9214ad8e459324D8157f1",
        "deploy_block": 7363975,
        "block_time": 3.038,
    },
}
UNISWAP_V3_FACTORY = "0x1F98431c8aD98523631AE4a59f267346ea31F984"
CHAIN = CHAINS[chain.id]
WOOFY = CHAIN["woofy"]
DEPLOY_BLOCK = CHAIN["deploy_block"]
BLOCK_TIME = CHAIN["block_time"]


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
    codes = list(ThreadPoolExecutor().map(get_code, snapshot))
    contracts = [addr for addr, code in zip(snapshot, codes) if code]
    replacements = {}

    for pool in tqdm(contracts, desc="identify pools"):
        try:
            factory = interface.IUniswapV2Pair(pool).factory()
        except ValueError:
            continue

        secho(f"Unwrapping LP {pool} => {factory}", fg="yellow")
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
        print(replacements)

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

    with open(f"reports/01-snapshots-{chain.id}.json", "wt") as f:
        json.dump(snapshots, f, indent=2)

    secho("Check addresses for being LP contracts", fg="yellow")
    print(valmap(len, snapshots))
    unique = set(concat(snapshots.values()))
    print(len(unique), "uniques")

    for epoch, block in epochs.items():
        secho(f"{epoch} Unwrap LP contracts", fg="yellow")

        print("before", len(snapshots[epoch]))
        replacements = {}

        replacements.update(unwrap_lp_tokens(snapshots[epoch], block, MIN_BALANCE))

        if chain.id == 1:
            replacements.update(unwrap_uniswap_v3(snapshots[epoch], block))

        print("repl", replacements)
        snapshots[epoch] = unwrap_balances(snapshots[epoch], replacements)
        print("after", len(snapshots[epoch]))

    with open(f"reports/02-snapshots-{chain.id}.json", "wt") as f:
        json.dump(snapshots, f, indent=2)


def stats():
    data = json.load(open("reports/02-weights.json"))
    data = {x: y / 1e12 for x, y in data.items() if y / 1e12 >= 2000}
    print(tabulate(Counter(data).most_common(100)))
    print(len(data))


def check():
    blocks = list(generate_snapshot_blocks())
    print(len(blocks))
    print(blocks)


def get_block_time():
    block_diff = chain.height - DEPLOY_BLOCK
    time_diff = chain[-1].timestamp - chain[DEPLOY_BLOCK].timestamp
    print(time_diff / block_diff)


def distribution():
    from glob import glob

    chances = Counter()
    for f in glob("reports/01-snapshots-*.json"):
        data = json.load(open(f))
        for block in data:
            for user in data[block]:
                chances[user] += 1

    with open("reports/02-chances.json", "wt") as f:
        json.dump(dict(chances.most_common()), f, indent=2)

    from toolz import groupby, valmap

    for a, b in sorted(valmap(len, groupby(chances.get, chances)).items()):
        print(f"{a} {b}")
