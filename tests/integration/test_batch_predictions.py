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

import pytest
import pandas as pd
import numpy as np

from datarobotx.idp.batch_predictions import get_update_or_create_batch_prediction_job
from datarobotx.idp.datasets import get_or_create_dataset_from_df
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
    """ Random deployment ID """
    return "6671e1c5a536861654e445a3"

@pytest.fixture
def schedule():
    return {"month": ["*"], "day_of_month": ["*"], "day_of_week": ["*"], "hour": [0], "minute": [0]}

@pytest.fixture
def batch_prediction_job(dataset, deployment_id):
    return {
        "num_concurrent": 4,
        "intake_settings": {
            "type": "dataset",
            "dataset_id": dataset,
        },
        "deployment_id": deployment_id
    }

# dr_endpoint, dr_token, schedule are pytest fixtures in other locations
def test_create_update_batch_prediction(
    dr_endpoint, dr_token, deployment_id, batch_prediction_job, schedule,
):
    job_id_1 = get_update_or_create_batch_prediction_job(
        endpoint=dr_endpoint,
        token=dr_token,
        deployment_id=deployment_id,
        batch_prediction_job=batch_prediction_job,
        enabled=True,
        name="pytest #1",
        schedule=schedule
    )
    job_id_2 = get_update_or_create_batch_prediction_job(
        endpoint=dr_endpoint,
        token=dr_token,
        deployment_id=deployment_id,
        batch_prediction_job=batch_prediction_job,
        enabled=False,
        name="pytest #1",
        schedule=schedule
    )
    assert job_id_1 == job_id_2

    job_id_3 = get_update_or_create_batch_prediction_job(
        endpoint=dr_endpoint,
        token=dr_token,
        deployment_id=deployment_id,
        batch_prediction_job=batch_prediction_job,
        enabled=True,
        name="pytest #2",
        schedule=schedule
    )
    assert job_id_3 != job_id_2
