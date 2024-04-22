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

import pandas as pd
import pytest

from datarobotx.idp.datasets import get_or_create_dataset_from_df, get_or_create_dataset_from_file
from datarobotx.idp.use_cases import get_or_create_use_case


@pytest.fixture
def use_case(dr_endpoint, dr_token, cleanup_dr):
    with cleanup_dr("useCases/"):
        yield get_or_create_use_case(
            dr_endpoint,
            dr_token,
            "pytest use case",
        )


@pytest.fixture
def cleanup_env(cleanup_dr):
    with cleanup_dr("datasets/", id_attribute="datasetId"):
        yield


@pytest.fixture
def dummy_df(tmp_path):
    return pd.DataFrame(
        {"num_legs": [2, 4, 8, 0], "num_wings": [2, 0, 0, 0], "num_specimen_seen": [10, 2, 1, 8]},
        index=["falcon", "dog", "spider", "fish"],
    )


@pytest.fixture
def dummy_dataset_file(tmp_path, dummy_df):
    path = tmp_path / "py_test.csv"
    dummy_df.to_csv(path)
    return str(path)


def test_get_or_create_from_file(dr_endpoint, dr_token, use_case, dummy_dataset_file, cleanup_env):
    ds_id_1 = get_or_create_dataset_from_file(
        dr_endpoint,
        dr_token,
        use_case,
        "pytest dataset #1",
        dummy_dataset_file,
    )
    assert len(ds_id_1)

    ds_id_2 = get_or_create_dataset_from_file(
        dr_endpoint,
        dr_token,
        use_case,
        "pytest dataset #1",
        dummy_dataset_file,
    )
    assert ds_id_1 == ds_id_2

    ds_id_3 = get_or_create_dataset_from_file(
        dr_endpoint,
        dr_token,
        use_case,
        "pytest dataset #2",
        dummy_dataset_file,
    )
    assert ds_id_1 != ds_id_3


def test_get_or_create_from_df(dr_endpoint, dr_token, use_case, dummy_df, cleanup_env):
    ds_id_1 = get_or_create_dataset_from_df(
        dr_endpoint,
        dr_token,
        use_case,
        "pytest dataset #1",
        dummy_df,
    )
    assert len(ds_id_1)

    ds_id_2 = get_or_create_dataset_from_df(
        dr_endpoint,
        dr_token,
        use_case,
        "pytest dataset #1",
        dummy_df,
    )
    assert ds_id_1 == ds_id_2

    ds_id_3 = get_or_create_dataset_from_df(
        dr_endpoint,
        dr_token,
        use_case,
        "pytest dataset #2",
        dummy_df,
    )
    assert ds_id_1 != ds_id_3
