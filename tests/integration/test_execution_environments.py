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

from datarobotx.idp.execution_environments import get_or_create_execution_environment


@pytest.fixture
def cleanup_env(cleanup_dr, debug_override):
    with cleanup_dr("executionEnvironments/", debug_override=debug_override):
        yield


def test_get_or_create(dr_endpoint, dr_token, cleanup_env):
    env_id_1 = get_or_create_execution_environment(dr_endpoint, dr_token, "pytest env #1")
    assert len(env_id_1)

    env_id_2 = get_or_create_execution_environment(dr_endpoint, dr_token, "pytest env #1")
    assert env_id_1 == env_id_2

    env_id_3 = get_or_create_execution_environment(dr_endpoint, dr_token, "pytest env #2")
    assert env_id_1 != env_id_3
