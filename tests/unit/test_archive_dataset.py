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


@pytest.fixture
def pathlib_path(tmp_path):
    p = tmp_path / "to_tar"
    p.mkdir()
    (p / "foo.txt").write_text("foobar")
    (p / "dummy_dir").mkdir()
    return p


@pytest.fixture(params=["path", "str", "tempdir"])
def folder_path(request, pathlib_path):
    if request.param == "path":
        return pathlib_path
    elif request.param == "str":
        return str(pathlib_path.resolve())
    else:
        d = tempfile.TemporaryDirectory()
        copytree(pathlib_path, d.name, dirs_exist_ok=True)
        return d


@pytest.fixture(params=["tar", "gztar", "zip"])
def storage_path(tmp_path, request):
    p = tmp_path / "storage"
    p.mkdir()
    if request.param == "tar":
        p = p / "foo.tar"
    elif request.param == "gztar":
        p = p / "foo.tar.gz"
    elif request.param == "zip":
        p = p / "foo.zip"
    else:
        raise ValueError("unexpected storage type")
    return str(p.resolve())


def test_path_dataset(folder_path, storage_path):
    catalog = DataCatalog.from_config(
        {
            "foo_ds": {
                "type": "datarobotx.idp.common.archive_dataset.ArchiveDataset",
                "filepath": storage_path,
            }
        }
    )
    catalog.save("foo_ds", folder_path)

    captured_path = []

    def node():
        p = catalog.load("foo_ds")
        assert (p / "foo.txt").read_text() == "foobar"
        assert (p / "dummy_dir").exists()
        captured_path.append(str(p.resolve()))

    node()
    # validate directory is cleaned-up upon garbage collection of the path object
    assert not Path(captured_path[0]).exists()
