#
# Copyright 2024 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# DataRobot, Inc.
#
# This is proprietary source code of DataRobot, Inc. and its
# affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
# https://www.datarobot.com/wp-content/uploads/2021/07/DataRobot-Tool-and-Utility-Agreement.pdf

"""Hashing utilities."""

from datetime import date
from hashlib import sha256
import inspect
import os
from pathlib import Path
from struct import pack
from typing import Any, Mapping, Sequence

import pandas as pd
from pandas.util import hash_pandas_object

from datarobot.models.api_object import APIObject

HASHING_ALGORITHM = sha256
CHECKSUM_FILE_EXTENSION = ".sha256"
TRUNCATE_HASH_TO = 7

_NONE_REPRESENTATION = 0xFCA86420


def int_to_bytes(number: int) -> bytes:
    """Get bytes representation of an int.

    Thanks stackoverflow
    """
    return number.to_bytes(
        length=(8 + (number + (number < 0)).bit_length()) // 8, byteorder="big", signed=True
    )


def get_hash(*args: Any, **kwargs: Any) -> str:
    """Hash common python built-ins."""
    hasher = HASHING_ALGORITHM()
    for arg in args:
        if isinstance(arg, bytes) or isinstance(arg, memoryview):
            pass
        elif arg is None:
            arg = int_to_bytes(_NONE_REPRESENTATION)
        elif isinstance(arg, str):
            arg = arg.encode("utf-8")
        elif isinstance(arg, int):
            arg = int_to_bytes(arg)
        elif isinstance(arg, float):
            arg = pack("f", arg)
        elif isinstance(arg, Mapping):
            d = {k: arg[k] for k in sorted(arg.keys())}
            arg = get_hash(**d).encode("utf-8")
        elif isinstance(arg, Sequence):
            arg = get_hash(*arg).encode("utf-8")
        elif isinstance(arg, date):
            arg = arg.isoformat().encode("utf-8")
        elif isinstance(arg, Path) and arg.is_file():
            with open(arg, "rb") as f:
                arg = ""
                for chunk in iter(lambda: f.read(8192), b""):
                    arg = get_hash(chunk, arg)
                arg = arg.encode("utf-8")
        elif isinstance(arg, Path) and arg.is_dir():
            base_path = arg
            arg = ""
            for root, dirs, files in os.walk(base_path):
                dirs.sort()  # force deterministic traversal
                arg = get_hash(str(Path(root).relative_to(base_path)), *dirs, *files, arg)
                for filename in sorted(files):
                    with open(os.path.join(root, filename), "rb") as f:
                        for chunk in iter(lambda: f.read(8192), b""):
                            arg = get_hash(chunk, arg)
            arg = arg.encode("utf-8")

        elif callable(arg):
            arg = inspect.getsource(arg).encode("utf-8")
        elif isinstance(arg, pd.DataFrame):
            arg = get_hash(
                hash_pandas_object(arg.columns).to_numpy().data,
                hash_pandas_object(arg.dtypes).to_numpy().data,
                hash_pandas_object(arg.index).to_numpy().data,
                hash_pandas_object(arg).to_numpy().data,
            )
            arg = arg.encode("utf-8")
        elif isinstance(arg, APIObject):
            d = {k: v for k, v in arg.__dict__.items() if not k.startswith("_") and not callable(v)}
            arg = get_hash(d).encode("utf-8")
        else:
            raise TypeError(f"Cannot tokenize object of type {type(arg)}")
        hasher.update(arg)
    for key in kwargs:
        hasher.update(key.encode("utf-8"))
        hasher.update(get_hash(kwargs[key]).encode("utf-8"))
    return hasher.hexdigest()[:TRUNCATE_HASH_TO]
