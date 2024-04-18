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
import yaml

import datarobot as dr
from datarobotx.idp.custom_jobs import get_replace_or_create_custom_job


@pytest.fixture()
def custom_job_exec_env():
    return "659bf1626529ceb502d12ae2"  # custom metric 3.9 drop-in env


@pytest.fixture
def runtime_parameter_definitions():
    return [
        {
            "fieldName": "STRING_PARAMETER",
            "type": "string",
            "value": "foobar",
        }
    ]


@pytest.fixture
def pythonic_runtime_parameters(runtime_parameter_definitions):
    d = runtime_parameter_definitions[0].copy()
    d["field_name"] = d.pop("fieldName")
    return [d]


@pytest.fixture
def metadata(runtime_parameter_definitions):
    return {
        "name": "pytest custom job runtime params",
        "runtimeParameterDefinitions": [
            {key: value for key, value in param.items() if key != "value"}
            for param in runtime_parameter_definitions
        ],
    }


@pytest.fixture
def schedule():
    return {"month": ["*"], "day_of_month": ["*"], "day_of_week": ["*"], "hour": [0], "minute": [0]}


@pytest.fixture
def folder_path(tmp_path, metadata):
    readme = tmp_path / "README.md"
    readme.write_text("foobar")
    readme2 = tmp_path / "README2.md"
    readme2.write_text("foobar2")
    start_sh = tmp_path / "start.sh"
    start_sh.write_text("echo 'hello world'")
    metadata_yaml = tmp_path / "metadata.yaml"
    with open(metadata_yaml, "w") as f:
        yaml.dump(metadata, f)
    return tmp_path


@pytest.fixture()
def cleanup_apps(cleanup_dr):
    with cleanup_dr("customJobs/"):
        yield


def test_get_or_create(
    dr_endpoint,
    dr_token,
    custom_job_exec_env,
    folder_path,
    pythonic_runtime_parameters,
    cleanup_apps,
    schedule,
):
    custom_job_1 = get_replace_or_create_custom_job(
        dr_endpoint,
        dr_token,
        "pytest custom job 1",
        folder_path,
        "start.sh",
        environment_id=custom_job_exec_env,
        runtime_parameter_values=pythonic_runtime_parameters,
    )
    assert len(custom_job_1)

    custom_job_2 = get_replace_or_create_custom_job(
        dr_endpoint,
        dr_token,
        "pytest custom job 1",
        folder_path,
        "start.sh",
        environment_id=custom_job_exec_env,
        runtime_parameter_values=pythonic_runtime_parameters,
    )
    assert custom_job_1 == custom_job_2

    # add new file
    (folder_path / "README3.md").write_text("foobar3")
    custom_job_3 = get_replace_or_create_custom_job(
        dr_endpoint,
        dr_token,
        "pytest custom job 1",
        folder_path,
        "start.sh",
        environment_id=custom_job_exec_env,
        runtime_parameter_values=pythonic_runtime_parameters,
    )
    # new file exists
    assert "README3.md" in {
        file["fileName"]
        for file in dr.Client(endpoint=dr_endpoint, token=dr_token)
        .get(url=f"customJobs/{custom_job_3}/")
        .json()["items"]
    }
    # and updating occurred in place
    assert custom_job_1 == custom_job_3

    custom_job_4 = get_replace_or_create_custom_job(
        dr_endpoint,
        dr_token,
        "pytest custom job 2",
        folder_path,
        "start.sh",
        environment_id=custom_job_exec_env,
        runtime_parameter_values=pythonic_runtime_parameters,
        schedule=schedule,
    )
    assert custom_job_1 != custom_job_4
