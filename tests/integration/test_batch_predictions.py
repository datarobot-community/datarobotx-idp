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

import datarobot as dr
from datarobotx.idp.autopilot import get_or_create_autopilot_run
from datarobotx.idp.batch_predictions import update_or_create_batch_prediction_job
from datarobotx.idp.datasets import get_or_create_dataset_from_df
from datarobotx.idp.registered_model_versions import (
    get_or_create_registered_leaderboard_model_version,
)
from datarobotx.idp.use_cases import get_or_create_use_case


@pytest.fixture
def analyze_and_model_config():
    return {
        "target": "is_bad",
        "mode": "quick",
        "worker_count": -1,
    }


@pytest.fixture
def df():
    return (
        pd.read_csv(
            "https://s3.amazonaws.com/datarobot_public_datasets/10K_2007_to_2011_Lending_Club_Loans_v2_mod_80.csv"
        )
        .groupby("is_bad")
        .sample(n=50, random_state=42)
    )


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
def autopilot_model(dr_endpoint, dr_token, use_case, dataset, analyze_and_model_config, cleanup_dr):
    with cleanup_dr("projects/", paginated=False):
        yield get_or_create_autopilot_run(
            dr_endpoint,
            dr_token,
            "pytest autopilot",
            dataset,
            analyze_and_model_config=analyze_and_model_config,
            use_case=use_case,
        )


@pytest.fixture()
def recommended_model(autopilot_model):
    return dr.ModelRecommendation.get(autopilot_model).model_id


@pytest.fixture
def registered_model_name(dr_token_hash):
    return f"pytest datarobot registered model {dr_token_hash}"


@pytest.fixture
def deployment_name(dr_token_hash):
    return "pytest datarobot deployment #{i}"


@pytest.fixture
def registered_model_version(
    cleanup_dr, dr_endpoint, dr_token, recommended_model, registered_model_name
):
    with cleanup_dr("registeredModels/"):
        yield get_or_create_registered_leaderboard_model_version(
            dr_endpoint, dr_token, recommended_model, registered_model_name
        )


@pytest.fixture
def deployment_id(
    cleanup_dr, registered_model_version, registered_model_name, default_prediction_server_id
):
    with cleanup_dr("deployments/"):
        yield dr.Deployment.create_from_registered_model_version(
            model_package_id=registered_model_version,
            label=registered_model_name,
            default_prediction_server_id=default_prediction_server_id,
        ).id


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
        "deployment_id": deployment_id,
    }


@pytest.fixture
def cleanup_batch_predictions(cleanup_dr, registered_model_version):
    with cleanup_dr("job-definitions/"):
        yield


# dr_endpoint, dr_token, schedule are pytest fixtures in other locations
def test_create_update_batch_prediction(
    dr_endpoint,
    dr_token,
    deployment_id,
    batch_prediction_job,
    schedule,
):
    job_id_1 = update_or_create_batch_prediction_job(
        endpoint=dr_endpoint,
        token=dr_token,
        deployment_id=deployment_id,
        batch_prediction_job=batch_prediction_job,
        enabled=True,
        name="pytest batch #1",
        schedule=schedule,
    )
    job_id_2 = update_or_create_batch_prediction_job(
        endpoint=dr_endpoint,
        token=dr_token,
        deployment_id=deployment_id,
        batch_prediction_job=batch_prediction_job,
        enabled=False,
        name="pytest batch #1",
        schedule=schedule,
    )
    assert job_id_1 == job_id_2

    job_id_3 = update_or_create_batch_prediction_job(
        endpoint=dr_endpoint,
        token=dr_token,
        deployment_id=deployment_id,
        batch_prediction_job=batch_prediction_job,
        enabled=True,
        name="pytest batch #2",
        schedule=schedule,
    )
    assert job_id_3 != job_id_2
