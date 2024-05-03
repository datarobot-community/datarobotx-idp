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
import tempfile
import zipfile

import pytest
import yaml

import datarobot as dr
from datarobotx.idp.credentials import get_replace_or_create_credential
from datarobotx.idp.custom_model_versions import (
    get_or_create_custom_model_version,
    get_or_create_custom_model_version_from_previous,
)
from datarobotx.idp.custom_models import get_or_create_custom_model


@pytest.fixture()
def dummy_credential(dr_endpoint, dr_token, cleanup_dr):
    with cleanup_dr("credentials/", id_attribute="credentialId"):
        name = "pytest_credential"
        credential_id = get_replace_or_create_credential(
            dr_endpoint, dr_token, name, "api_token", api_token="foobar"
        )
        yield (name, credential_id)


@pytest.fixture
def custom_model(cleanup_dr, dr_endpoint, dr_token):
    with cleanup_dr("customModels/"):
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
def another_folder_path_with_metadata(model_metadata, another_folder_path):
    p = pathlib.Path(another_folder_path)
    p = p / "model-metadata.yaml"
    with open(p, "w") as f:
        yaml.dump(model_metadata, f)
    return another_folder_path


@pytest.fixture
def folder_path_with_metadata_and_reqs(folder_path_with_metadata):
    requirements = "scikit-learn==1.4.0"
    p = pathlib.Path(folder_path_with_metadata)
    (p / "requirements.txt").write_text(requirements)
    return folder_path_with_metadata


@pytest.fixture
def updated_folder_path_with_metadata_and_reqs(another_folder_path_with_metadata):
    requirements = "scikit-learn==1.4.2"
    p = pathlib.Path(another_folder_path_with_metadata)
    (p / "requirements.txt").write_text(requirements)
    p = p / "files"
    p.mkdir()
    (p / "file1").write_text("foo")
    return another_folder_path_with_metadata


@pytest.fixture
def cleanup_model_ver(cleanup_dr, custom_model):
    with cleanup_dr(f"customModels/{custom_model}/versions"):
        yield


def test_get_or_create(
    dr_endpoint,
    dr_token,
    cleanup_model_ver,
    custom_model,
    sklearn_drop_in_env,
    pythonic_runtime_parameters,
    folder_path_with_metadata_and_reqs,
    updated_folder_path_with_metadata_and_reqs,
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
    updated_version_1 = get_or_create_custom_model_version_from_previous(
        endpoint=dr_endpoint,
        token=dr_token,
        custom_model_id=custom_model,
        base_environment_id=sklearn_drop_in_env,
        folder_path=updated_folder_path_with_metadata_and_reqs,
        runtime_parameter_values=pythonic_runtime_parameters,
        maximum_memory=4096 * 1024 * 1024,
    )
    assert updated_version_1 != model_ver_id_5

    updated_version_2 = get_or_create_custom_model_version_from_previous(
        endpoint=dr_endpoint,
        token=dr_token,
        custom_model_id=custom_model,
        base_environment_id=sklearn_drop_in_env,
        folder_path=updated_folder_path_with_metadata_and_reqs,
        runtime_parameter_values=pythonic_runtime_parameters,
        maximum_memory=4096 * 1024 * 1024,
    )

    assert updated_version_1 == updated_version_2

    with tempfile.TemporaryDirectory() as d:
        zip_file_path = pathlib.Path(d) / "model.zip"
        dr.CustomModelVersion.get(
            custom_model_id=custom_model, custom_model_version_id=updated_version_2
        ).download(zip_file_path)

        # iterate over the zip file contents to find file1
        zip_ref = zipfile.ZipFile(zip_file_path, "r")
        assert "files/file1" in zip_ref.namelist()
        assert "requirements.txt" in zip_ref.namelist()
        assert zip_ref.read("files/file1") == b"foo"
        assert zip_ref.read("requirements.txt") == b"scikit-learn==1.4.2"
