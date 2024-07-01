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

from kedro.io import DataCatalog
import pytest
import yaml

from datarobotx.idp.common.asset_resolver import merge_asset_paths, prepare_yaml_content


@pytest.fixture()
def assets_path(tmp_path):
    p = tmp_path / "base_location"
    p.mkdir()
    (p / "foo.txt").write_text("foobar")
    (p / "dummy_dir").mkdir()
    (p / "dummy_dir" / "dummy_file.txt").write_text("dummy")
    return p


@pytest.fixture()
def secondary_asset_path(tmp_path):
    p = tmp_path / "secondary_location.yaml"
    output = prepare_yaml_content(entry_one="baz", entry_two="qux")
    p.write_bytes(output)
    return p


@pytest.fixture()
def storage_path(tmp_path):
    p = tmp_path / "storage"
    p.mkdir()
    return str(p.resolve())


@pytest.fixture()
def secondary_storage_path(tmp_path):
    p = tmp_path / "secondary_storage"
    p.mkdir()
    return str(p.resolve())


@pytest.fixture()
def final_storage_path(tmp_path):
    p = tmp_path / "final_storage"
    p.mkdir()
    p = p / "final.zip"
    return str(p.resolve())


def test_asset_resolvers(
    assets_path,
    secondary_asset_path,
    storage_path,
    secondary_storage_path,
    final_storage_path,
):
    catalog = DataCatalog.from_config(
        {
            "folder_of_assets": {
                "type": "datarobotx.idp.common.path_dataset.PathDataset",
                "filepath": storage_path,
            },
            "secondary_file": {
                "type": "datarobotx.idp.common.path_dataset.PathDataset",
                "filepath": secondary_storage_path,
            },
            "folder_of_everything": {
                "type": "datarobotx.idp.common.archive_dataset.ArchiveDataset",
                "filepath": final_storage_path,
            },
        }
    )

    catalog.save("folder_of_assets", assets_path)
    catalog.save("secondary_file", secondary_asset_path)

    merged_outputs = merge_asset_paths(assets_path, secondary_asset_path)
    catalog.save("folder_of_everything", merged_outputs)

    captured_path = []

    def node():
        p = catalog.load("folder_of_everything")
        assert (p / "foo.txt").read_text() == "foobar"
        assert (p / "dummy_dir").exists()
        assert (p / "dummy_dir" / "dummy_file.txt").read_text() == "dummy"
        with open(p / "secondary_location.yaml", "r") as f:
            assert yaml.safe_load(f) == {"entry_one": "baz", "entry_two": "qux"}
        captured_path.append(str(p.resolve()))

    node()
    # validate directory / file is cleaned-up upon garbage collection of the path object
    assert not Path(captured_path[0]).exists()
