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
from datarobotx.idp.datasets import get_or_create_dataset_from_df
from datarobotx.idp.deployments import (
    _lookup_registered_model_version,
    get_or_create_deployment_from_registered_model_version,
    get_replace_or_create_deployment_from_registered_model,
)
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
def use_case(dr_endpoint, dr_token, cleanup_dr, debug_override):
    with cleanup_dr("useCases/", debug_override=debug_override):
        yield get_or_create_use_case(
            dr_endpoint,
            dr_token,
            "pytest use case",
        )


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
def dataset(dr_endpoint, dr_token, df, use_case, cleanup_dr, debug_override):
    with cleanup_dr("datasets/", id_attribute="datasetId", debug_override=debug_override):
        yield get_or_create_dataset_from_df(
            dr_endpoint,
            dr_token,
            use_case_id=use_case,
            name="pytest_test_autopilot_dataset",
            data_frame=df,
        )


@pytest.fixture
def autopilot_model(
    dr_endpoint, dr_token, use_case, dataset, analyze_and_model_config, cleanup_dr, debug_override
):
    with cleanup_dr("projects/", paginated=False, debug_override=debug_override):
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


@pytest.fixture()
def other_model(autopilot_model):
    return dr.Project.get(autopilot_model).get_models()[1].id


@pytest.fixture
def registered_model_name(dr_token_hash):
    return f"pytest datarobot registered model {dr_token_hash}"


@pytest.fixture
def deployment_name(dr_token_hash):
    return "pytest datarobot deployment #{i}"


@pytest.fixture
def registered_model_version(
    cleanup_dr, dr_endpoint, dr_token, recommended_model, registered_model_name, debug_override
):
    with cleanup_dr("registeredModels/", debug_override=debug_override):
        yield get_or_create_registered_leaderboard_model_version(
            dr_endpoint, dr_token, recommended_model, registered_model_name
        )


@pytest.fixture
def cleanup_deployments(cleanup_dr, registered_model_version, debug_override):
    with cleanup_dr("deployments/", debug_override=debug_override):
        yield


def test_get_or_create(
    dr_endpoint,
    dr_token,
    cleanup_deployments,
    registered_model_version,
    default_prediction_server_id,
    deployment_name,
):
    deployment_1 = get_or_create_deployment_from_registered_model_version(
        dr_endpoint,
        dr_token,
        registered_model_version,
        deployment_name.format(i=1),
        default_prediction_server_id=default_prediction_server_id,
    )
    assert len(deployment_1)

    deployment_2 = get_or_create_deployment_from_registered_model_version(
        dr_endpoint,
        dr_token,
        registered_model_version,
        deployment_name.format(i=1),
        default_prediction_server_id=default_prediction_server_id,
    )
    assert deployment_1 == deployment_2

    deployment_3 = get_or_create_deployment_from_registered_model_version(
        dr_endpoint,
        dr_token,
        registered_model_version,
        deployment_name.format(i=2),
        default_prediction_server_id=default_prediction_server_id,
    )
    assert deployment_1 != deployment_3


def test_get_create_or_replace(
    dr_endpoint,
    dr_token,
    other_model,
    registered_model_version,
    registered_model_name,
    default_prediction_server_id,
    deployment_name,
    cleanup_deployments,
):
    deployment_1 = get_replace_or_create_deployment_from_registered_model(
        dr_endpoint,
        dr_token,
        registered_model_version,
        registered_model_name,
        deployment_name.format(i=1),
        default_prediction_server_id=default_prediction_server_id,
    )
    assert len(deployment_1)

    deployment_2 = get_replace_or_create_deployment_from_registered_model(
        dr_endpoint,
        dr_token,
        registered_model_version,
        registered_model_name,
        deployment_name.format(i=1),
        default_prediction_server_id=default_prediction_server_id,
    )
    assert deployment_1 == deployment_2

    registered_model_version_2 = get_or_create_registered_leaderboard_model_version(
        dr_endpoint, dr_token, other_model, registered_model_name
    )
    deployment_3 = get_replace_or_create_deployment_from_registered_model(
        dr_endpoint,
        dr_token,
        registered_model_version_2,
        registered_model_name,
        deployment_name.format(i=1),
        default_prediction_server_id=default_prediction_server_id,
    )
    # swapping in a new registered model version replaces deployment instead of creating new
    assert deployment_1 == deployment_3

    registered_model_version_3, _ = _lookup_registered_model_version(
        dr_endpoint, dr_token, deployment_3
    )
    # and the replacement actually brings in the correct new custom model version
    assert registered_model_version_3 == registered_model_version_2

    # but changes other than updating registered model version always create a new deployment
    deployment_4 = get_replace_or_create_deployment_from_registered_model(
        dr_endpoint,
        dr_token,
        registered_model_version_2,
        registered_model_name,
        deployment_name.format(i=2),
        default_prediction_server_id=default_prediction_server_id,
    )
    assert deployment_1 != deployment_4
