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

from datarobotx.idp.custom_application_source import (  # type: ignore
    get_or_create_custom_application_source,
    get_or_create_custom_application_source_version,
)


@pytest.fixture
def requirements() -> str:
    return """\
streamlit==1.29.0
"""


@pytest.fixture
def app_py() -> str:
    return """\
import streamlit as st

st.write('Hello World!')
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


@pytest.fixture
def custom_application_source(
    dr_endpoint: str, dr_token: str, cleanup_dr: Any
) -> Generator[str, None, None]:
    with cleanup_dr("customApplicationSources/"):
        yield get_or_create_custom_application_source(
            endpoint=dr_endpoint,
            token=dr_token,
            name="pytest custom application source",
        )


@pytest.fixture
def cleanup_env_ver(cleanup_dr: Any, custom_application_source: str) -> Generator[None, None, None]:
    with cleanup_dr(f"customApplicationSources/{custom_application_source}/versions"):
        yield


def test_get_or_create(
    dr_endpoint: str,
    dr_token: str,
    cleanup_env_ver: Any,
    custom_application_source: str,
    app_context_path: str,
) -> None:
    env_ver_id_1 = get_or_create_custom_application_source_version(
        endpoint=dr_endpoint,
        token=dr_token,
        custom_application_source_id=custom_application_source,
        folder_path=app_context_path,
        label="pytest app source version",
    )
    assert len(env_ver_id_1)

    env_ver_id_2 = get_or_create_custom_application_source_version(
        endpoint=dr_endpoint,
        token=dr_token,
        custom_application_source_id=custom_application_source,
        folder_path=app_context_path,
        label="pytest app source version",
    )
    assert env_ver_id_1 == env_ver_id_2

    env_ver_id_3 = get_or_create_custom_application_source_version(
        endpoint=dr_endpoint,
        token=dr_token,
        custom_application_source_id=custom_application_source,
        folder_path=app_context_path,
        label="pytest app source version alt",
    )
    assert env_ver_id_1 != env_ver_id_3
