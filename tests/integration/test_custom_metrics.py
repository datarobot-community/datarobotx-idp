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

import datarobot as dr
from datarobotx.idp.custom_metrics import get_update_or_create_custom_metric
from datarobotx.idp.custom_model_versions import get_or_create_custom_model_version
from datarobotx.idp.custom_models import get_or_create_custom_model
from datarobotx.idp.deployments import (
    get_or_create_deployment_from_registered_model_version,
)
from datarobotx.idp.registered_model_versions import (
    get_or_create_registered_custom_model_version,
)


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
def registered_model_version(
    cleanup_dr, dr_endpoint, dr_token, custom_model_version, registered_model_name
):
    with cleanup_dr("registeredModels/"):
        yield get_or_create_registered_custom_model_version(
            dr_endpoint, dr_token, custom_model_version, registered_model_name
        )


@pytest.fixture
def deployment(
    cleanup_dr,
    dr_endpoint,
    dr_token,
    registered_model_version,
    default_prediction_server_id,
):
    with cleanup_dr("deployments/"):
        yield get_or_create_deployment_from_registered_model_version(
            dr_endpoint,
            dr_token,
            registered_model_version,
            "pytest custom deployment #1",
            default_prediction_server_id=default_prediction_server_id,
        )


@pytest.fixture
def custom_metric_arguments():
    return {
        "name": "pytest custom metric",
        "directionality": "higherIsBetter",
        "units": "foo",
        "type": "average",
        "baseline_values": [{"value": 1}],
        "is_model_specific": False,
    }


def test_build_custom_metrics(dr_endpoint, dr_token, deployment, custom_metric_arguments):
    metric_1 = get_update_or_create_custom_metric(
        dr_endpoint,
        dr_token,
        deployment,
        **custom_metric_arguments,
    )

    assert len(metric_1)

    metric_2 = get_update_or_create_custom_metric(
        dr_endpoint,
        dr_token,
        deployment,
        **custom_metric_arguments,
        description="A different description",
    )
    assert metric_1 == metric_2

    resp = (
        dr.Client(dr_token, dr_endpoint)
        .get(f"deployments/{deployment}/customMetrics/{metric_1}")
        .json()
    )
    assert resp["description"] == "A different description"

    custom_metric_arguments["name"] = "pytest custom metric2"
    metric_3 = get_update_or_create_custom_metric(
        dr_endpoint,
        dr_token,
        deployment,
        **custom_metric_arguments,
    )
    assert metric_1 != metric_3
