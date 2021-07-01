import math
from concurrent.futures import ThreadPoolExecutor
from functools import partial

from brownie import Contract
from eth_abi.exceptions import InsufficientDataBytes
from toolz import concat, partition_all
from tqdm import tqdm


def fetch_multicall(calls, block=None):
    multicall = Contract("0x5BA1e12693Dc8F9c48aAD8770482f4739bEeD696")

    multicall_input = []
    fn_list = []
    decoded = []

    for contract, fn_name, *fn_inputs in calls:
        fn = getattr(contract, fn_name)

        if hasattr(fn, "_get_fn_from_args"):
            fn = fn._get_fn_from_args(fn_inputs)

        fn_list.append(fn)
        multicall_input.append((contract, fn.encode_input(*fn_inputs)))

    response = multicall.tryAggregate.call(
        False, multicall_input, block_identifier=block
    )

    for fn, (ok, data) in zip(fn_list, response):
        try:
            assert ok, "call failed"
            decoded.append(fn.decode_output(data))
        except (AssertionError, InsufficientDataBytes):
            decoded.append(None)

    return decoded


def fetch_multicall_batched(calls, block=None, batch_size=1000):
    func = partial(fetch_multicall, block=block)
    return list(
        concat(
            tqdm(
                ThreadPoolExecutor().map(func, partition_all(batch_size, calls)),
                total=math.ceil(len(calls) / batch_size),
            )
        )
    )
