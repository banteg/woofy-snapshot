import math
from collections import Counter
from fractions import Fraction
from brownie import Contract
from scripts.snapshot import UNISWAP_V3_FACTORY

from camera_shy.common import (
    NFT_POSITION_MANAGER,
    UNISWAP_V3_FACTORY,
    eth_call,
    get_code,
    memory,
)
from camera_shy.multicall import fetch_multicall, fetch_multicall_batched


@memory.cache()
def is_uniswap_v3_pool(address):
    if not get_code(address):
        return False
    try:
        return eth_call(address, "factory()(address)") == UNISWAP_V3_FACTORY
    except ValueError:
        return False


@memory.cache()
def fetch_uniswap_v3_positions(block):
    manager = Contract(NFT_POSITION_MANAGER)
    total_supply = manager.totalSupply(block_identifier=block)
    ids = fetch_multicall_batched(
        [[manager, "tokenByIndex", i] for i in range(total_supply)], block=block
    )
    positions = fetch_multicall_batched(
        [[manager, "positions", i] for i in ids], block=block
    )
    return {token_id: position.dict() for token_id, position in zip(ids, positions)}


def filter_positions_of_pool(pool, positions):
    token0, token1, fee = fetch_multicall(
        [[pool, key] for key in ["token0", "token1", "fee"]]
    )
    return {
        i: pos
        for i, pos in positions.items()
        if (pos["token0"], pos["token1"], pos["fee"]) == (token0, token1, fee)
    }


def unwrap_liquidity(pool, token, positions, block=None, min_balance=0):
    manager = Contract(NFT_POSITION_MANAGER)
    positions = filter_positions_of_pool(pool, positions)
    total_liquidity = sum(pos["liquidity"] for pos in positions.values())
    total_balance = token.balanceOf(pool, block_identifier=block)
    owners = fetch_multicall([[manager, "ownerOf", i] for i in positions], block=block)

    user_balances = Counter()

    for i, owner in zip(positions, owners):
        user_balances[owner] += (
            Fraction(positions[i]["liquidity"], total_liquidity) * total_balance
        )

    return {
        user: int(tokens)
        for user, tokens in user_balances.most_common()
        if tokens >= min_balance
    }
