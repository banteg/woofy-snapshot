import math
from collections import Counter
from fractions import Fraction

from brownie import Contract, chain, interface
from joblib import Memory
from scripts.snapshot import UNISWAP_V3_FACTORY

from camera_shy.multicall import fetch_multicall, fetch_multicall_batched

memory = Memory(f"cache/{chain.id}", verbose=0)

UNISWAP_V3_FACTORY = "0x1F98431c8aD98523631AE4a59f267346ea31F984"
NFT_POSITION_MANAGER = "0xC36442b4a4522E871399CD717aBDD847Ab11FE88"


@memory.cache()
def is_uniswap_v3_pool(address):
    try:
        factory = interface.IUniswapV3Pool(address).factory()
        return factory == UNISWAP_V3_FACTORY
    except ValueError:
        return False


def fetch_uniswap_v3_positions(block=None):
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
    owners = fetch_multicall_batched(
        [[manager, "ownerOf", i] for i in positions], block=block
    )

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
