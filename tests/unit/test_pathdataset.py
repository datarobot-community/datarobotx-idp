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

from pathlib import Path
from shutil import copytree
import tempfile

from kedro.io import DataCatalog
import pytest


@pytest.fixture(params=["folder", "file"])
def pathlib_path(request, tmp_path):
    p = tmp_path / "base_location"
    p.mkdir()
    if request.param == "file":
        (p / "foo.txt").write_text("foobar")
        return p / "foo.txt", request.param
    elif request.param == "folder":
        (p / "foo.txt").write_text("foobar")
        (p / "dummy_dir").mkdir()
        (p / "dummy_dir" / "dummy_file.txt").write_text("dummy")
        return p, request.param


@pytest.fixture(params=["path", "str", "tempdir"])
def asset_path(request, pathlib_path):
    path, save_type = pathlib_path
    if request.param == "path":
        return path
    elif request.param == "str":
        return str(path.resolve())
    elif request.param == "tempdir" and save_type == "file":
        pytest.skip("tempdir is not supported for file")
    else:
        d = tempfile.TemporaryDirectory()
        copytree(path, d.name, dirs_exist_ok=True)
        return d


@pytest.fixture()
def storage_path(tmp_path, pathlib_path):
    _, save_type = pathlib_path
    if save_type == "file":
        p = tmp_path / "foo.txt"
        p.write_text("dummy value")
    else:
        p = tmp_path / "storage"
        p.mkdir()
    return str(p.resolve())


def test_path_dataset(asset_path, storage_path, pathlib_path):
    _, save_type = pathlib_path
    catalog = DataCatalog.from_config(
        {
            "foo_ds": {
                "type": "datarobotx.idp.common.path_dataset.PathDataset",
                "filepath": storage_path,
            }
        }
    )

    catalog.save("foo_ds", asset_path)

    captured_path = []

    def node():
        p = catalog.load("foo_ds")
        if save_type == "file":
            assert p.read_text() == "foobar"
        if save_type == "folder":
            assert (p / "foo.txt").read_text() == "foobar"
            assert (p / "dummy_dir").exists()
            assert (p / "dummy_dir" / "dummy_file.txt").read_text() == "dummy"
        captured_path.append(str(p.resolve()))

    node()
    if save_type == "folder":
        # validate directory is cleaned-up upon garbage collection of the path object
        assert not Path(captured_path[0]).exists()
