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

import numpy as np
import pandas as pd
import pytest

from datarobotx.idp.datasets import get_or_create_dataset_from_df
from datarobotx.idp.retraining_policies import update_or_create_retraining_policy
from datarobotx.idp.use_cases import get_or_create_use_case


@pytest.fixture
def df():
    return pd.DataFrame(np.random.randint(0, 100, size=(20, 4)), columns=list("ABCD"))


@pytest.fixture
def use_case(dr_endpoint, dr_token, cleanup_dr):
    with cleanup_dr("useCases/"):
        yield get_or_create_use_case(
            dr_endpoint,
            dr_token,
            "pytest use case",
        )


# Using pytest fixture dataset...
@pytest.fixture
def dataset(dr_endpoint, dr_token, df, use_case, cleanup_dr):
    with cleanup_dr("datasets/", id_attribute="datasetId"):
        yield get_or_create_dataset_from_df(
            dr_endpoint,
            dr_token,
            use_case_id=use_case,
            name="pytest_test_autopilot_dataset",
            data_frame=df,
        )


@pytest.fixture
def deployment_id():
    """Random deployment ID, where training has not been configured"""
    return "6671e1c5a536861654e445a3"


def test_create_retraining_policy(dr_endpoint, dr_token, deployment_id, dataset):
    retraining_id_1 = update_or_create_retraining_policy(
        endpoint=dr_endpoint,
        token=dr_token,
        deployment_id=deployment_id,
        name="test",
        dataset_id=dataset,
    )
    print(retraining_id_1)
