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

import pathlib

import pytest
import yaml

import datarobot as dr
from datarobotx.idp.credentials import get_replace_or_create_credential
from datarobotx.idp.custom_model_versions import get_or_create_custom_model_version
from datarobotx.idp.custom_models import get_or_create_custom_model


@pytest.fixture()
def dummy_credential(dr_endpoint, dr_token, cleanup_dr, debug_override):
    with cleanup_dr("credentials/", id_attribute="credentialId", debug_override=debug_override):
        name = "pytest_credential"
        credential_id = get_replace_or_create_credential(
            dr_endpoint, dr_token, name, "api_token", api_token="foobar"
        )
        yield (name, credential_id)


@pytest.fixture
def custom_model(cleanup_dr, dr_endpoint, dr_token, debug_override):
    with cleanup_dr("customModels/", debug_override=debug_override):
        yield get_or_create_custom_model(
            dr_endpoint, dr_token, "pytest custom model", "Regression", target_name="foo"
        )


@pytest.fixture
def runtime_parameter_values(dummy_credential):
    return [
        {
            "fieldName": dummy_credential[0],
            "type": "credential",
            "value": dummy_credential[1],
        }
    ]


@pytest.fixture
def pythonic_runtime_parameters(runtime_parameter_values):
    d = runtime_parameter_values[0].copy()
    d["field_name"] = d.pop("fieldName")
    return [d]


@pytest.fixture
def model_metadata(runtime_parameter_values):
    return {
        "name": "pytest custom model",
        "type": "inference",
        "targetType": "regression",
        "runtimeParameterDefinitions": [
            {key: value for key, value in param.items() if key != "value"}
            for param in runtime_parameter_values
        ],
    }


@pytest.fixture
def folder_path_with_metadata(model_metadata, folder_path):
    p = pathlib.Path(folder_path)
    p = p / "model-metadata.yaml"
    with open(p, "w") as f:
        yaml.dump(model_metadata, f)
    return folder_path


@pytest.fixture
def folder_path_with_metadata_and_reqs(folder_path_with_metadata):
    requirements = "scikit-learn==1.4.0"
    p = pathlib.Path(folder_path_with_metadata)
    (p / "requirements.txt").write_text(requirements)
    return folder_path_with_metadata


@pytest.fixture
def cleanup_model_ver(cleanup_dr, custom_model, debug_override):
    with cleanup_dr(f"customModels/{custom_model}/versions", debug_override=debug_override):
        yield


def test_get_or_create(
    dr_endpoint,
    dr_token,
    cleanup_model_ver,
    custom_model,
    sklearn_drop_in_env,
    pythonic_runtime_parameters,
    folder_path_with_metadata_and_reqs,
):
    model_ver_id_1 = get_or_create_custom_model_version(
        dr_endpoint,
        dr_token,
        custom_model,
        sklearn_drop_in_env,
        folder_path_with_metadata_and_reqs,
    )
    assert len(model_ver_id_1)

    model_ver_id_2 = get_or_create_custom_model_version(
        dr_endpoint,
        dr_token,
        custom_model,
        sklearn_drop_in_env,
        folder_path_with_metadata_and_reqs,
    )
    assert model_ver_id_1 == model_ver_id_2

    model_ver_id_3 = get_or_create_custom_model_version(
        dr_endpoint,
        dr_token,
        custom_model,
        sklearn_drop_in_env,
        folder_path_with_metadata_and_reqs,
        maximum_memory=4096 * 1024 * 1024,
    )
    assert model_ver_id_1 != model_ver_id_3

    model_ver_id_4 = get_or_create_custom_model_version(
        dr_endpoint,
        dr_token,
        custom_model,
        sklearn_drop_in_env,
        folder_path_with_metadata_and_reqs,
        runtime_parameter_values=pythonic_runtime_parameters,
        maximum_memory=4096 * 1024 * 1024,
    )
    assert model_ver_id_4 != model_ver_id_3

    model_ver_id_5 = get_or_create_custom_model_version(
        dr_endpoint,
        dr_token,
        custom_model,
        sklearn_drop_in_env,
        folder_path_with_metadata_and_reqs,
        runtime_parameter_values=pythonic_runtime_parameters,
        maximum_memory=4096 * 1024 * 1024,
    )
    assert model_ver_id_5 == model_ver_id_4

    dr.Client(endpoint=dr_endpoint, token=dr_token)
    assert (
        dr.CustomModelVersionDependencyBuild.get_build_info(
            custom_model, model_ver_id_5
        ).build_status
        == "success"
    )
