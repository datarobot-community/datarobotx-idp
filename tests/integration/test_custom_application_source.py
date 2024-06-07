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

from typing import Any, Generator

import pytest

from datarobotx.idp.custom_application_source import (  # type: ignore
    get_or_create_custom_application_source,
)


@pytest.fixture
def cleanup_env(cleanup_dr: Any) -> Generator[None, None, None]:
    with cleanup_dr("customApplicationSources/"):
        yield


def test_get_or_create(dr_endpoint: str, dr_token: str, cleanup_env: Any) -> None:
    env_id_1 = get_or_create_custom_application_source(
        endpoint=dr_endpoint, token=dr_token, name="pytest env #1"
    )
    assert len(env_id_1)

    env_id_2 = get_or_create_custom_application_source(
        endpoint=dr_endpoint, token=dr_token, name="pytest env #1"
    )
    assert env_id_1 == env_id_2

    env_id_3 = get_or_create_custom_application_source(
        endpoint=dr_endpoint, token=dr_token, name="pytest env #2"
    )
    assert env_id_1 != env_id_3
