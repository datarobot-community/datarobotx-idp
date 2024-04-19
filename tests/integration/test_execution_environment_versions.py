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

import os

import pytest

from datarobotx.idp.execution_environment_versions import (
    get_or_create_execution_environment_version,
)
from datarobotx.idp.execution_environments import get_or_create_execution_environment


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


@pytest.fixture
def execution_environment(dr_endpoint, dr_token, cleanup_dr):
    with cleanup_dr("executionEnvironments/"):
        yield get_or_create_execution_environment(
            dr_endpoint,
            dr_token,
            "pytest custom execution environment",
        )


@pytest.fixture
def cleanup_env_ver(cleanup_dr, execution_environment):
    with cleanup_dr(f"executionEnvironments/{execution_environment}/versions"):
        yield


def test_get_or_create(
    dr_endpoint,
    dr_token,
    cleanup_env_ver,
    execution_environment,
    docker_context_path,
):
    env_ver_id_1 = get_or_create_execution_environment_version(
        dr_endpoint,
        dr_token,
        execution_environment,
        docker_context_path,
        label="pytest execution env version",
    )
    assert len(env_ver_id_1)

    env_ver_id_2 = get_or_create_execution_environment_version(
        dr_endpoint,
        dr_token,
        execution_environment,
        docker_context_path,
        label="pytest execution env version",
    )
    assert env_ver_id_1 == env_ver_id_2

    env_ver_id_3 = get_or_create_execution_environment_version(
        dr_endpoint,
        dr_token,
        execution_environment,
        docker_context_path,
        label="pytest execution env version alt",
    )
    assert env_ver_id_1 != env_ver_id_3
