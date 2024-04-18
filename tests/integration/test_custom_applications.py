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
import os

import pytest

from datarobotx.idp.custom_applications import (
    _find_existing_custom_app,
    get_replace_or_create_custom_app_from_env,
)
from datarobotx.idp.execution_environment_versions import (
    get_or_create_execution_environment_version,
)
from datarobotx.idp.execution_environments import get_or_create_execution_environment


@pytest.fixture()
def custom_app_exec_env(dr_endpoint, dr_token, cleanup_dr):
    with cleanup_dr("executionEnvironments/"):
        yield get_or_create_execution_environment(
            dr_endpoint,
            dr_token,
            "pytest custom app execution environment",
            use_cases=["customApplication"],
        )


@pytest.fixture
def dockerfile():
    return """\
FROM python:3.9.5-slim-buster
WORKDIR /app/
COPY app.py /app/
COPY requirements.txt /app/
RUN pip install -U pip && pip install -r requirements.txt

ARG port=80
ENV STREAMLIT_SERVER_PORT ${port}
EXPOSE ${port}
ENV PIP_NO_CACHE_DIR=1 \
    PIP_PREFER_BINARY=1

ENTRYPOINT ["streamlit", "run", "app.py", "--server.address=0.0.0.0"]
"""


@pytest.fixture
def requirements():
    return """\
streamlit==1.29.0
"""


@pytest.fixture
def app_py():
    return """\
import streamlit as st

st.write('Hello World!')
"""


@pytest.fixture
def docker_context_path(tmp_path, requirements, dockerfile, app_py):
    ts = 1701797283
    p = tmp_path / "Dockerfile"
    p.write_text(dockerfile)
    os.utime(p, (ts, ts))
    p = tmp_path / "requirements.txt"
    p.write_text(requirements)
    os.utime(p, (ts, ts))
    p = tmp_path / "app.py"
    p.write_text(app_py)
    os.utime(p, (ts, ts))
    return str(tmp_path.resolve())


@pytest.fixture()
def docker_context_path_mod(tmp_path, requirements, dockerfile, app_py):
    ts = 1701797283
    p = tmp_path / "Dockerfile"
    p.write_text(dockerfile)
    os.utime(p, (ts, ts))
    p = tmp_path / "requirements.txt"
    p.write_text("streamlit==1.28.0")
    os.utime(p, (ts, ts))
    p = tmp_path / "app.py"
    p.write_text(app_py)
    os.utime(p, (ts, ts))
    return str(tmp_path.resolve())


@pytest.fixture()
def custom_app_exec_env_version(
    dr_endpoint, dr_token, cleanup_dr, custom_app_exec_env, docker_context_path
):
    with cleanup_dr(f"executionEnvironments/{custom_app_exec_env}/versions"):
        yield get_or_create_execution_environment_version(
            dr_endpoint,
            dr_token,
            custom_app_exec_env,
            docker_context_path,
        )


@pytest.fixture()
def cleanup_apps(cleanup_dr):
    with cleanup_dr("customApplications/"):
        yield


def test_get_or_create(
    dr_endpoint,
    dr_token,
    custom_app_exec_env_version,
    docker_context_path_mod,
    custom_app_exec_env,
    cleanup_apps,
):
    custom_app_id_1 = get_replace_or_create_custom_app_from_env(
        dr_endpoint,
        dr_token,
        "pytest custom app 1",
        custom_app_exec_env,
        custom_app_exec_env_version,
    )
    assert len(custom_app_id_1)

    custom_app_id_2 = get_replace_or_create_custom_app_from_env(
        dr_endpoint,
        dr_token,
        "pytest custom app 1",
        custom_app_exec_env,
        custom_app_exec_env_version,
    )
    assert custom_app_id_1 == custom_app_id_2

    custom_app_id_3 = get_replace_or_create_custom_app_from_env(
        dr_endpoint,
        dr_token,
        "pytest custom app 2",
        custom_app_exec_env,
        custom_app_exec_env_version,
    )
    assert custom_app_id_1 != custom_app_id_3

    # create a new execution environment_version
    custom_app_exec_env_version_mod = get_or_create_execution_environment_version(
        dr_endpoint,
        dr_token,
        custom_app_exec_env,
        docker_context_path_mod,
    )
    custom_app_id_4 = get_replace_or_create_custom_app_from_env(
        dr_endpoint,
        dr_token,
        "pytest custom app 2",
        custom_app_exec_env,
        custom_app_exec_env_version_mod,
    )
    # New exec env version triggers app delete and re-create
    assert custom_app_id_3 != custom_app_id_4
    # Old app is gone
    with pytest.raises(KeyError):
        _find_existing_custom_app(
            dr_endpoint,
            dr_token,
            name="pytest custom app 2",
            env_version_id=custom_app_exec_env_version,
        )
