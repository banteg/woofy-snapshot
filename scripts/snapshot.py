import json
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from fractions import Fraction
from glob import glob
from itertools import count

from brownie import ZERO_ADDRESS, Contract, chain, interface, web3
from brownie.utils.output import build_tree
from camera_shy import masterchef, uniswap_v3
from camera_shy.common import (
    block_after_timestamp,
    decode_logs,
    eth_call,
    filter_contracts,
    get_token_transfers,
    merge_balances,
    transfers_to_balances,
    unwrap_balances,
)
from click import secho, style
from eth_abi.exceptions import InsufficientDataBytes
from toolz import concat, groupby, unique, valmap
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
    uniswap_v3_positions = uniswap_v3.fetch_uniswap_v3_positions(block)

    uniswap_pools = [
        user for user in tqdm(snapshot) if uniswap_v3.is_uniswap_v3_pool(user)
    ]
    print(
        build_tree(
            [
                [
                    style(f"Uniswap V3 Pools", fg="bright_magenta"),
                    *uniswap_pools,
                ]
            ]
        )
    )

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
        except (ValueError, InsufficientDataBytes):
            continue
        else:
            # cache the abi to skip pulling from explorer
            contract = interface.IUniswapV2Pair(pool)
            pools.append(pool)

    if pools:
        print(build_tree([[style("Uniswap V2 Pools", fg="bright_magenta"), *pools]]))

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


def unwrap_masterchef(snapshot, lp_replacements):
    contracts = filter_contracts(snapshot)
    chefs = [contract for contract in contracts if masterchef.is_masterchef(contract)]
    print(f"{len(snapshot)} users -> {len(contracts)} contracts -> {len(chefs)} chefs")
    replacements = {}
    # chef -> pid -> lp
    pids = {
        chef: masterchef.find_pids_with_token(chef, WOOFY)
        for chef in tqdm(chefs, desc="finding chef pids")
    }
    print(
        build_tree(
            [
                [
                    style("MasterChef contracts", fg="bright_yellow"),
                    *[[chef, *map(str, pids[chef])] for chef in pids],
                ]
            ]
        )
    )

    for chef in pids:
        deposits = masterchef.get_masterchef_deposits(chef, pids[chef], DEPLOY_BLOCK)
        # pid -> user -> balance
        balances = masterchef.chef_events_to_staked_balances(deposits, chain.height)
        replacements[chef] = Counter()

        for pid in pids[chef]:
            lp_supply = sum(balances[pid].values())
            token_supply = lp_replacements[pids[chef][pid]].get(chef, 0)
            for user, balance in balances[pid].items():
                replacements[chef][user] += int(
                    Fraction(balance, lp_supply) * token_supply
                )

        replacements[chef] = {
            user: balance
            for user, balance in replacements[chef].most_common()
            if balance >= MIN_BALANCE
        }

    return replacements


def main():
    epochs = generate_snapshot_blocks(SNAPSHOT_START, SNAPSHOT_INTERVAL)
    snapshots = {}

    logs = get_token_transfers(WOOFY, DEPLOY_BLOCK)
    events = decode_logs(list(logs))

    if VAULT:
        vault_logs = get_token_transfers(VAULT, DEPLOY_BLOCK)
        vault_events = decode_logs(list(vault_logs))

    for epoch, block in epochs.items():
        secho(f"Photographing {epoch}", fg="green", bold=True)

        snapshots[epoch] = transfers_to_balances(events, block, MIN_BALANCE)

        if VAULT:
            vault_additions = transfers_to_balances(vault_events, block, MIN_BALANCE)
            snapshots[epoch] = merge_balances(snapshots[epoch], vault_additions)

        lp_replacements = {}
        lp_replacements.update(unwrap_lp_tokens(snapshots[epoch], block, MIN_BALANCE))

        if chain.id == 1:
            lp_replacements.update(unwrap_uniswap_v3(snapshots[epoch], block))

        # apply lp balances before seaching for masterchefs
        snapshots[epoch] = unwrap_balances(snapshots[epoch], lp_replacements)

        chef_replacements = unwrap_masterchef(snapshots[epoch], lp_replacements)
        snapshots[epoch] = unwrap_balances(snapshots[epoch], chef_replacements)

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
