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
import pathlib
from typing import Any, Generator

import pytest

from datarobotx.idp.custom_application_source import (
    get_or_create_custom_application_source,
    get_or_create_custom_application_source_version,
)
from datarobotx.idp.custom_applications import (
    _find_existing_custom_app,
    get_replace_or_create_custom_app,
)


@pytest.fixture()
def custom_app_name(dr_token_hash: str) -> str:
    return "pytest custom app #{i} " + dr_token_hash


@pytest.fixture()
def custom_application_source(
    dr_endpoint: str,
    dr_token: str,
    cleanup_dr: Any,
) -> Generator[str, None, None]:
    with cleanup_dr("customApplicationSources/"):
        yield get_or_create_custom_application_source(
            endpoint=dr_endpoint,
            token=dr_token,
            name="pytest custom app source",
        )


@pytest.fixture
def base_environment_id() -> str:
    return "6542cd582a9d3d51bf4ac71e"


@pytest.fixture
def requirements() -> str:
    return """\
streamlit==1.29.0
"""


@pytest.fixture
def start_script() -> str:
    return """\
#!/usr/bin/env bash
#
#  Copyright 2024 DataRobot, Inc. and its affiliates.
#
#  All rights reserved.
#  This is proprietary source code of DataRobot, Inc. and its affiliates.
#  Released under the terms of DataRobot Tool and Utility Agreement.
#
echo "Starting App"

streamlit run app.py
"""


@pytest.fixture
def app_py() -> str:
    return """\
import streamlit as st

st.write('Hello World!')
"""


@pytest.fixture
def app_context_path(
    tmp_path: pathlib.Path, requirements: str, app_py: str, start_script: str
) -> str:
    ts = 1701797283
    p = tmp_path / "requirements.txt"
    p.write_text(requirements)
    os.utime(p, (ts, ts))
    p = tmp_path / "app.py"
    p.write_text(app_py)
    os.utime(p, (ts, ts))
    p = tmp_path / "start-app.sh"
    p.write_text(start_script)
    os.utime(p, (ts, ts))
    return str(tmp_path.resolve())


@pytest.fixture()
def app_context_path_mod(tmp_path: pathlib.Path, app_py: str, start_script: str) -> str:
    ts = 1701797283
    p = tmp_path / "requirements.txt"
    p.write_text("streamlit==1.28.0")
    os.utime(p, (ts, ts))
    p = tmp_path / "app.py"
    p.write_text(app_py)
    os.utime(p, (ts, ts))
    p = tmp_path / "start-app.sh"
    p.write_text(start_script)
    os.utime(p, (ts, ts))
    return str(tmp_path.resolve())


@pytest.fixture()
def custom_application_source_version(
    dr_endpoint: str,
    dr_token: str,
    cleanup_dr: Any,
    custom_application_source: str,
    app_context_path: str,
    base_environment_id: str,
) -> Generator[str, None, None]:
    with cleanup_dr(f"customApplicationSources/{custom_application_source}/versions"):
        yield get_or_create_custom_application_source_version(
            endpoint=dr_endpoint,
            token=dr_token,
            custom_application_source_id=custom_application_source,
            folder_path=app_context_path,
            base_environment_id=base_environment_id,
        )


@pytest.fixture()
def cleanup_apps(cleanup_dr: Any) -> Generator[None, None, None]:
    with cleanup_dr("customApplications/"):
        yield


def test_get_or_create(
    dr_endpoint: str,
    dr_token: str,
    custom_application_source_version: str,
    app_context_path_mod: str,
    custom_application_source: str,
    custom_app_name: str,
    base_environment_id: str,
    cleanup_apps: Any,
) -> None:
    custom_app_id_1 = get_replace_or_create_custom_app(
        endpoint=dr_endpoint,
        token=dr_token,
        name=custom_app_name.format(i=1),
        custom_application_source_version_id=custom_application_source_version,
    )
    assert len(custom_app_id_1)

    custom_app_id_2 = get_replace_or_create_custom_app(
        endpoint=dr_endpoint,
        token=dr_token,
        name=custom_app_name.format(i=1),
        custom_application_source_version_id=custom_application_source_version,
    )
    assert custom_app_id_1 == custom_app_id_2

    custom_app_id_3 = get_replace_or_create_custom_app(
        endpoint=dr_endpoint,
        token=dr_token,
        name=custom_app_name.format(i=2),
        custom_application_source_version_id=custom_application_source_version,
    )
    assert custom_app_id_1 != custom_app_id_3

    # create a new execution environment_version
    custom_app_exec_env_version_mod = get_or_create_custom_application_source_version(
        endpoint=dr_endpoint,
        token=dr_token,
        custom_application_source_id=custom_application_source,
        folder_path=app_context_path_mod,
        base_environment_id=base_environment_id,
    )
    custom_app_id_4 = get_replace_or_create_custom_app(
        endpoint=dr_endpoint,
        token=dr_token,
        name=custom_app_name.format(i=2),
        custom_application_source_version_id=custom_app_exec_env_version_mod,
    )
    # New exec env version triggers app delete and re-create
    assert custom_app_id_3 != custom_app_id_4
    # Old app is gone
    with pytest.raises(KeyError):
        _find_existing_custom_app(
            dr_endpoint,
            dr_token,
            name=custom_app_name.format(i=2),
            env_version_id=custom_application_source_version,
        )
