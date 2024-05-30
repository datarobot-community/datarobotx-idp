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

import datarobot as dr
from datarobotx.idp.credentials import get_replace_or_create_credential  # type: ignore
from datarobotx.idp.custom_application_source import (  # type: ignore
    get_or_create_custom_application_source,
)
from datarobotx.idp.custom_application_source_version import (  # type: ignore
    get_or_create_custom_application_source_version,
    get_or_create_custom_application_source_version_from_previous,
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
def metadata() -> str:
    return """\
name: runtime-params

runtimeParameterDefinitions:
- fieldName: SOME_TEXT
  type: string
- fieldName: SOME_CREDENTIALS
  type: credential
"""


@pytest.fixture
def credentials(dr_endpoint: str, dr_token: str, cleanup_dr: Any) -> Generator[str, None, None]:
    with cleanup_dr("credentials/"):
        yield get_replace_or_create_credential(
            endpoint=dr_endpoint,
            token=dr_token,
            name="pytest credential",
            credential_type="api_token",
            api_token="pytest token",
        )


@pytest.fixture
def app_context_path(
    tmp_path: pathlib.Path, requirements: str, app_py: str, start_script: str, metadata: str
) -> str:
    ts = 1701797283
    folder_path = tmp_path / "app"
    folder_path.mkdir()
    p = folder_path / "requirements.txt"
    p.write_text(requirements)
    os.utime(p, (ts, ts))
    p = folder_path / "app.py"
    p.write_text(app_py)
    os.utime(p, (ts, ts))
    p = folder_path / "start-app.sh"
    p.write_text(start_script)
    os.utime(p, (ts, ts))
    p = folder_path / "metadata.yaml"
    p.write_text(metadata)
    os.utime(p, (ts, ts))
    return str(folder_path.resolve())


@pytest.fixture
def additional_metadata_context_path(
    tmp_path: pathlib.Path,
) -> str:
    ts = 1701797283
    folder_path = tmp_path / "additional_app"
    folder_path.mkdir()
    p = folder_path / "foo.txt"
    p.write_text("foo")
    os.utime(p, (ts, ts))
    return str(folder_path.resolve())


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
def custom_application_source_2(
    dr_endpoint: str, dr_token: str, cleanup_dr: Any
) -> Generator[str, None, None]:
    with cleanup_dr("customApplicationSources/"):
        yield get_or_create_custom_application_source(
            endpoint=dr_endpoint,
            token=dr_token,
            name="pytest custom application source_2",
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
    additional_metadata_context_path: str,
    credentials: str,
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


def test_get_or_create_with_runtime_params(
    dr_endpoint: str,
    dr_token: str,
    cleanup_env_ver: Any,
    custom_application_source_2: str,
    app_context_path: str,
    additional_metadata_context_path: str,
    credentials: str,
) -> None:
    client = dr.Client(endpoint=dr_endpoint, token=dr_token)  # type: ignore

    env_ver_id_1 = get_or_create_custom_application_source_version(
        endpoint=dr_endpoint,
        token=dr_token,
        custom_application_source_id=custom_application_source_2,
        runtime_parameter_values=[
            {"field_name": "SOME_TEXT", "type": "string", "value": "foo"},
            {"field_name": "SOME_CREDENTIALS", "type": "credential", "value": credentials},
        ],
        folder_path=app_context_path,
    )

    assert env_ver_id_1

    runtime_params = client.get(
        f"customApplicationSources/{custom_application_source_2}/versions/{env_ver_id_1}"
    ).json()["runtimeParameters"]
    some_text_metadata = next(item for item in runtime_params if item["fieldName"] == "SOME_TEXT")
    some_creds_metadata = next(
        item for item in runtime_params if item["fieldName"] == "SOME_CREDENTIALS"
    )

    assert some_text_metadata["currentValue"] == "foo"
    assert some_creds_metadata["currentValue"] == credentials

    env_ver_id_2 = get_or_create_custom_application_source_version(
        endpoint=dr_endpoint,
        token=dr_token,
        custom_application_source_id=custom_application_source_2,
        runtime_parameter_values=[
            {"field_name": "SOME_TEXT", "type": "string", "value": "foo"},
            {"field_name": "SOME_CREDENTIALS", "type": "credential", "value": credentials},
        ],
        folder_path=app_context_path,
    )

    assert env_ver_id_1 == env_ver_id_2

    folder_contents_old = [
        item["fileName"]
        for item in client.get(
            f"customApplicationSources/{custom_application_source_2}/versions/{env_ver_id_2}"
        ).json()["items"]
    ]

    env_ver_id_3 = get_or_create_custom_application_source_version_from_previous(
        endpoint=dr_endpoint,
        token=dr_token,
        custom_application_source_id=custom_application_source_2,
        folder_path=additional_metadata_context_path,
    )
    assert env_ver_id_3 != env_ver_id_2

    folder_contents_new = [
        item["fileName"]
        for item in client.get(
            f"customApplicationSources/{custom_application_source_2}/versions/{env_ver_id_3}"
        ).json()["items"]
    ]

    assert folder_contents_old + ["foo.txt"] == folder_contents_new

    env_ver_id_4 = get_or_create_custom_application_source_version_from_previous(
        endpoint=dr_endpoint,
        token=dr_token,
        custom_application_source_id=custom_application_source_2,
        folder_path=additional_metadata_context_path,
    )

    assert env_ver_id_3 == env_ver_id_4
