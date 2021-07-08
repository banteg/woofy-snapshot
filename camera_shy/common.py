from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

from brownie import Contract, chain, web3
from brownie.network.event import _decode_logs
from joblib import Memory
from toolz import concat
from tqdm import tqdm
from web3.middleware.filter import block_ranges

from multicall import Call

UNISWAP_V3_FACTORY = "0x1F98431c8aD98523631AE4a59f267346ea31F984"
NFT_POSITION_MANAGER = "0xC36442b4a4522E871399CD717aBDD847Ab11FE88"

memory = Memory(f"cache/{chain.id}", verbose=0)
log_batch_size = {56: 1000}.get(chain.id, 10000)


def eth_call(target, function, *args):
    return Call(target, function, _w3=web3)(args)


@memory.cache()
def get_code(address):
    return web3.eth.get_code(address)


def filter_contracts(addresses):
    codes = ThreadPoolExecutor().map(get_code, addresses)
    return [
        addr
        for addr, code in tqdm(
            zip(addresses, codes), desc="finding contracts", total=len(addresses)
        )
        if code
    ]


@memory.cache()
def get_logs(address, topics, from_block, to_block):
    return web3.eth.get_logs(
        {
            "address": address,
            "topics": topics,
            "fromBlock": from_block,
            "toBlock": to_block,
        }
    )


def get_token_transfers(token, start_block):
    contract = Contract(token)
    yield from concat(
        get_logs(token, [contract.topics["Transfer"]], start, end)
        for start, end in tqdm(
            list(block_ranges(start_block, chain.height, log_batch_size)),
            desc="fetch logs",
        )
    )


def decode_logs(logs):
    """
    Decode logs to events and enrich them with additional info.
    """
    decoded = _decode_logs(logs)
    for i, log in enumerate(logs):
        setattr(decoded[i], "block_number", log["blockNumber"])
    return decoded


@memory.cache()
def block_after_timestamp(timestamp):
    """
    Find first block after timestamp using binary search.
    """
    if isinstance(timestamp, datetime):
        timestamp = timestamp.timestamp()

    height = chain.height
    lo, hi = 0, height

    while hi - lo > 1:
        mid = lo + (hi - lo) // 2
        if chain[mid].timestamp > timestamp:
            hi = mid
        else:
            lo = mid

    return hi if hi != height else None


def transfers_to_balances(events, snapshot_block, min_balance=0):
    """
    Convert Transfer logs to balances snapshot.
    """
    balances = Counter()

    for log in events:
        if log.block_number > snapshot_block:
            break
        sender, receiver, amount = log.values()
        balances[sender] -= amount
        balances[receiver] += amount

    return {
        user: balance
        for user, balance in balances.most_common()
        if balance >= min_balance
    }


def unwrap_balances(balances, replacements):
    for remove, additions in replacements.items():
        balances.pop(remove)
        for user, balance in additions.items():
            balances.setdefault(user, 0)
            balances[user] += balance
    return dict(Counter(balances).most_common())


def merge_balances(*many_balances):
    merged = Counter()
    for balances in many_balances:
        merged += balances
    return dict(merged.most_common())
