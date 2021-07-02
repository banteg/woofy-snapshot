from collections import Counter
from datetime import datetime

from brownie import chain, web3, Contract
from brownie.network.event import _decode_logs
from joblib import Memory
from toolz import concat
from tqdm import tqdm
from web3.middleware.filter import block_ranges

MULTICALL = "0x5BA1e12693Dc8F9c48aAD8770482f4739bEeD696"
SUSHISWAP_V2_FACTORY = "0xC0AEe478e3658e2610c5F7A4A2E1777cE9e4f2Ac"
UNISWAP_V2_FACTORY = "0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f"
UNISWAP_V3_FACTORY = "0x1F98431c8aD98523631AE4a59f267346ea31F984"
NFT_POSITION_MANAGER = "0xC36442b4a4522E871399CD717aBDD847Ab11FE88"

memory = Memory(f"cache/{chain.id}", verbose=0)
log_batch_size = {56: 1000}.get(chain.id, 10000)


@memory.cache()
def get_code(address):
    return web3.eth.get_code(address)


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
            list(block_ranges(start_block, chain.height, log_batch_size)), desc="fetch logs"
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
