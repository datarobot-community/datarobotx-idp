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

import datetime as dt

import pytest

from datarobotx.idp.custom_model_versions import get_or_create_custom_model_version
from datarobotx.idp.custom_models import get_or_create_custom_model
from datarobotx.idp.deployments import (
    _lookup_registered_model_version,
    get_or_create_deployment_from_registered_model_version,
    get_replace_or_create_deployment_from_registered_model,
)
from datarobotx.idp.registered_model_versions import get_or_create_registered_custom_model_version


@pytest.fixture
def custom_model(cleanup_dr, dr_endpoint, dr_token):
    with cleanup_dr("customModels/"):
        yield get_or_create_custom_model(
            dr_endpoint,
            dr_token,
            name="pytest custom model",
            target_type="Regression",
            target_name="foo",
        )


@pytest.fixture
def custom_model_version(
    cleanup_dr, custom_model, dr_endpoint, dr_token, folder_path, sklearn_drop_in_env
):
    with cleanup_dr(f"customModels/{custom_model}/versions/"):
        yield get_or_create_custom_model_version(
            dr_endpoint, dr_token, custom_model, sklearn_drop_in_env, folder_path
        )


@pytest.fixture
def registered_model_name():
    return "pytest custom registered model"


@pytest.fixture
def deployment_name():
    return "pytest datarobot deployment #{i}" + dt.datetime.now().isoformat()


@pytest.fixture
def registered_model_version(
    cleanup_dr, dr_endpoint, dr_token, custom_model_version, registered_model_name
):
    with cleanup_dr("registeredModels/"):
        yield get_or_create_registered_custom_model_version(
            dr_endpoint, dr_token, custom_model_version, registered_model_name
        )


@pytest.fixture
def cleanup_deployments(cleanup_dr, registered_model_version):
    with cleanup_dr("deployments/"):
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
    cleanup_deployments,
    custom_model,
    sklearn_drop_in_env,
    folder_path,
    registered_model_version,
    default_prediction_server_id,
    registered_model_name,
    deployment_name,
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

    custom_model_version_2 = get_or_create_custom_model_version(
        dr_endpoint,
        dr_token,
        custom_model,
        sklearn_drop_in_env,
        folder_path,
        maximum_memory=4096 * 1024 * 1024,
    )
    registered_model_version_2 = get_or_create_registered_custom_model_version(
        dr_endpoint, dr_token, custom_model_version_2, registered_model_name
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
