from brownie import Contract, chain, interface
from tqdm import trange, tqdm
from camera_shy.common import get_logs, decode_logs, memory, eth_call, filter_contracts
from web3.middleware.filter import block_ranges
from eth_abi import encode_single
from eth_utils import encode_hex
from toolz import concat
from concurrent.futures import ThreadPoolExecutor
from brownie.convert.datatypes import EthAddress
from collections import defaultdict, Counter

SPOOKY_CHEF = "0x2b2929E785374c651a81A63878Ab22742656DcDd"


@memory.cache()
def is_masterchef(address):
    try:
        return eth_call(address, "poolLength()(uint256)")
    except ValueError:
        return False


@memory.cache()
def contains_tokens(lp, token):
    try:
        # assume uniswap v2 style lp tokens
        lp_tokens = [
            EthAddress(eth_call(lp, f"{key}()(address)"))
            for key in ["token0", "token1"]
        ]
    except ValueError:
        lp_tokens = [lp]

    return token in lp_tokens


@memory.cache()
def find_pids_with_token(chef, token):
    chef = Contract(chef)
    if "lpToken" in chef.poolInfo(0).dict():
        data = [
            info["lpToken"]
            for info in ThreadPoolExecutor().map(
                chef.poolInfo, range(chef.poolLength())
            )
        ]
    else:
        # MiniChefV2 doesn't return lpToken
        data = list(ThreadPoolExecutor().map(chef.lpToken, range(chef.poolLength())))

    contains = ThreadPoolExecutor().map(lambda lp: contains_tokens(lp, token), data)
    return {pid: lp for pid, (lp, has) in enumerate(zip(data, contains)) if has}


def get_masterchef_deposits(chef, pids, start_block):
    chef = Contract(chef)
    topics = [
        [chef.topics[key] for key in ["Deposit", "Withdraw", "EmergencyWithdraw"]],
        None,
        [encode_hex(encode_single("uint256", pid)) for pid in pids],
    ]
    ranges = list(block_ranges(start_block, chain.height, 10000))
    func = lambda x: get_logs(str(chef), topics, x[0], x[1])
    tasks = ThreadPoolExecutor().map(func, ranges)
    logs = list(concat(tqdm(tasks, desc="fetch masterchef logs", total=len(ranges))))
    return decode_logs(logs)


def chef_events_to_staked_balances(events, snapshot_block):
    # pid -> user -> balance
    balances = defaultdict(Counter)

    for event in events:
        if event.block_number > snapshot_block:
            break
        if event.name == "Deposit":
            balances[event["pid"]][event["user"]] += event["amount"]
        elif event.name in ["Withdraw", "EmergencyWithdraw"]:
            balances[event["pid"]][event["user"]] -= event["amount"]

    return balances
