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

import json

import pandas as pd
import pytest

import datarobot as dr
from datarobotx.idp.autopilot import get_or_create_autopilot_run
from datarobotx.idp.datasets import get_or_create_dataset_from_df
from datarobotx.idp.deployments import get_or_create_deployment_from_registered_model_version
from datarobotx.idp.registered_model_versions import (
    get_or_create_registered_leaderboard_model_version,
)
from datarobotx.idp.retraining_policies import get_update_or_create_retraining_policy
from datarobotx.idp.use_cases import get_or_create_use_case


######################################
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
    cleanup_dr,
    registered_model_version,
    registered_model_name,
    default_prediction_server_id,
    dr_endpoint,
    dr_token,
):
    with cleanup_dr("deployments/"):
        yield get_or_create_deployment_from_registered_model_version(
            endpoint=dr_endpoint,
            token=dr_token,
            registered_model_version_id=registered_model_version,
            label=registered_model_name,
            default_prediction_server_id=default_prediction_server_id,
        )


def test_retraining_policy(dr_endpoint, dr_token, deployment_id, dataset):
    print(deployment_id)
    # Can we create policy
    retraining_id_1 = get_update_or_create_retraining_policy(
        endpoint=dr_endpoint,
        token=dr_token,
        deployment_id=deployment_id,
        name="post_test",
        dataset_id=dataset,
    )

    # If we "update" policy, is the ID the same?
    retraining_id_2 = get_update_or_create_retraining_policy(
        endpoint=dr_endpoint,
        token=dr_token,
        deployment_id=deployment_id,
        name="post_test",
        dataset_id=dataset,
    )
    assert retraining_id_1 == retraining_id_2

    # If we actually change a parameter, is the ID the same?
    retraining_id_3 = get_update_or_create_retraining_policy(
        endpoint=dr_endpoint,
        token=dr_token,
        deployment_id=deployment_id,
        name="patch_test",
        dataset_id=dataset,
        featureListStrategy="informative_features",
    )

    # Add test to assert the update occurred
    client = dr.Client(token=dr_token, endpoint=dr_endpoint)
    get_response = client.request(
        method="GET", url=f"deployments/{deployment_id}/retrainingPolicies/{retraining_id_3}"
    )
    policy_data = json.loads(get_response.text)
    assert policy_data["featureListStrategy"] == "informative_features"

    retraining_id = get_update_or_create_retraining_policy(
        endpoint=dr_endpoint,
        token=dr_token,
        deployment_id=deployment_id,
        name="test",
        dataset_id=dataset,
    )

    retraining_response_1 = client.get(
        f"deployments/{deployment_id}/retrainingPolicies/{retraining_id}"
    ).json()

    retraining_id_2 = get_update_or_create_retraining_policy(
        endpoint=dr_endpoint,
        token=dr_token,
        deployment_id=deployment_id,
        name="test",
        dataset_id=dataset,
    )

    retraining_response_2 = client.get(
        f"deployments/{deployment_id}/retrainingPolicies/{retraining_id_2}"
    ).json()

    assert retraining_response_1 == retraining_response_2
