#
# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# DataRobot, Inc.
#
# This is proprietary source code of DataRobot, Inc. and its
# affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
import numpy as np
import pandas as pd
import pytest

import datarobot as dr
from datarobotx.idp.projects import get_or_create_project_from_dataset


@pytest.fixture
def dummy_df():
    return pd.DataFrame(np.random.randint(0, 100, size=(20, 4)), columns=list("ABCD"))


@pytest.fixture
def dummy_dataset1(dummy_df, cleanup_dr):
    with cleanup_dr("datasets/", id_attribute="datasetId"):
        yield dr.Dataset.create_from_in_memory_data(  # type: ignore
            data_frame=dummy_df
        )


@pytest.fixture
def dummy_dataset2(dummy_df, cleanup_dr):
    with cleanup_dr("datasets/", id_attribute="datasetId"):
        yield dr.Dataset.create_from_in_memory_data(  # type: ignore
            data_frame=dummy_df
        )


@pytest.fixture
def cleanup_projects(cleanup_dr):
    with cleanup_dr("projects/", paginated=False):
        yield


def test_get_or_create(dr_endpoint, dr_token, dummy_dataset1, dummy_dataset2, cleanup_projects):
    project_id_1 = get_or_create_project_from_dataset(
        dr_endpoint,
        dr_token,
        "pytest project #1",
        str(dummy_dataset1.id),
    )
    assert len(project_id_1)

    project_id_2 = get_or_create_project_from_dataset(
        dr_endpoint,
        dr_token,
        "pytest project #1",
        str(dummy_dataset1.id),
    )
    assert project_id_1 == project_id_2

    project_id_3 = get_or_create_project_from_dataset(
        dr_endpoint,
        dr_token,
        "pytest project #1",
        str(dummy_dataset2.id),
    )
    assert project_id_1 != project_id_3
