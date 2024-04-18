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

from contextlib import contextmanager
import os
from pathlib import Path
import shutil
import string
import time

import numpy as np
import pandas as pd
import pytest

from datarobotx.idp.common.hashing import get_hash


@pytest.fixture
def simple_file(tmp_path):
    now = int(time.time())

    @contextmanager
    def factory():
        p = tmp_path / "foo.txt"
        p.write_text("foobar")
        os.utime(p, (now, now))
        archive_path = Path(
            shutil.make_archive(tmp_path / "foo", "zip", root_dir=tmp_path, base_dir=tmp_path)
        )
        yield archive_path
        p.unlink()
        archive_path.unlink()

    return factory


@pytest.fixture
def simple_folder(tmp_path):
    @contextmanager
    def factory(bonus_file=False, bonus_dir=False):
        base = tmp_path / "foobar"
        base.mkdir()
        (base / "bar" / "foo").mkdir(parents=True)
        (base / "bar" / "foobar").mkdir()
        (base / "foo.txt").write_text("foobar")
        (base / "bar" / "foo" / "foo.txt").write_text("foobar")
        if bonus_file:
            (base / "bar" / "bar").mkdir()
            (base / "bar" / "bar" / "foo.txt").write_text("foobar")
        if bonus_dir:
            (base / "foobar").mkdir()
        yield base
        shutil.rmtree(base)

    return factory


class TestHasher:
    def test_none(self):
        token = get_hash(None)
        assert all(c in string.hexdigits for c in token)

    def test_bytes(self):
        b = "hello world".encode("utf-8")
        token = get_hash(b)
        assert all(c in string.hexdigits for c in token)

    def test_bool(self):
        token = get_hash(True, False)
        assert all(c in string.hexdigits for c in token)

    def test_str(self):
        token = get_hash("hello world")
        assert all(c in string.hexdigits for c in token)

    def test_int(self):
        token = get_hash(52)
        assert all(c in string.hexdigits for c in token)

    def test_float(self):
        token = get_hash(52.0)
        assert all(c in string.hexdigits for c in token)

    def test_seq(self):
        token = get_hash([1, "hi", 5.0], ("foo", 3))
        assert all(c in string.hexdigits for c in token)

        token1 = get_hash([1, 2])
        token2 = get_hash([2, 1])
        assert token1 != token2

    def test_file_path(self, simple_file):
        with simple_file() as f:
            token1 = get_hash(f)
        assert all(c in string.hexdigits for c in token1)
        with simple_file() as f:
            token2 = get_hash(f)
        assert token1 == token2

    def test_dir_path(self, simple_folder):
        with simple_folder() as f:
            token1 = get_hash(f)
        assert all(c in string.hexdigits for c in token1)
        with simple_folder() as f:
            token2 = get_hash(f)
        assert token1 == token2
        with simple_folder(bonus_dir=True) as f:
            token3 = get_hash(f)
        assert token1 != token3
        with simple_folder(bonus_dir=True) as f:
            token4 = get_hash(f)
        assert token3 == token4
        with simple_folder(bonus_file=True) as f:
            token5 = get_hash(f)
        assert token1 != token5
        with simple_folder(bonus_file=True) as f:
            token6 = get_hash(f)
        assert token5 == token6

    def test_f(self):
        def foo():
            pass

        token1 = get_hash(foo)
        assert all(c in string.hexdigits for c in token1)

        def foo():
            pass

        token2 = get_hash(foo)
        assert token1 == token2

        def foo_mod():
            print("hello world")

        token3 = get_hash(foo_mod)
        assert token1 != token3

    def test_df(self):
        df1 = pd.DataFrame(
            {
                "A": 1.0,
                "B": pd.Timestamp("20130102"),
                "C": pd.Series(1, index=list(range(4)), dtype="float32"),
                "D": np.array([3] * 4, dtype="int32"),
                "E": pd.Categorical(["test", "train", "test", "train"]),
                "F": "foo",
            }
        )
        token1 = get_hash(df1)
        assert all(c in string.hexdigits for c in token1)

        df2 = pd.DataFrame(
            {
                "A": 1.0,
                "B": pd.Timestamp("20130102"),
                "C": pd.Series(1, index=list(range(4)), dtype="float32"),
                "D": np.array([3] * 4, dtype="int32"),
                "E": pd.Categorical(["test", "train", "test", "train"]),
                "F": "foo",
            }
        )
        token2 = get_hash(df2)
        assert token1 == token2

        df3 = df1.copy(deep=True)
        df3["A"] = 2.0
        token3 = get_hash(df3)
        assert token1 != token3

        df4 = df1.copy(deep=True)
        df4["F"] = "bar"
        token4 = get_hash(df4)
        assert token1 != token4

    def test_mapping(self):
        token = get_hash({"foo": "bar", "bar": [1, 2, 3.0]})
        assert all(c in string.hexdigits for c in token)

        token1 = get_hash({"foo": "bar", "bar": "foo"})
        token2 = get_hash({"bar": "foo", "foo": "bar"})
        assert token1 == token2

    def test_idempotent(self):
        token1 = get_hash({"foo": "bar", "bar": [1, 2, 3.0]})
        token2 = get_hash({"foo": "bar", "bar": [1, 2, 3.0]})
        assert token1 == token2
