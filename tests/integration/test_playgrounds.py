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

from datarobotx.idp.playgrounds import get_or_create_playground
from datarobotx.idp.use_cases import get_or_create_use_case


@pytest.fixture
def use_case(dr_endpoint, dr_token, cleanup_dr, debug_override):
    with cleanup_dr("useCases/", debug_override=debug_override):
        yield get_or_create_use_case(
            dr_endpoint,
            dr_token,
            "pytest use case",
        )


@pytest.fixture
def cleanup_env(cleanup_dr, debug_override):
    with cleanup_dr("genai/playgrounds/", debug_override=debug_override):
        yield


def test_get_or_create(dr_endpoint, dr_token, use_case, cleanup_env):
    pg_1 = get_or_create_playground(
        dr_endpoint,
        dr_token,
        "pytest playground #1",
        use_case=use_case,
    )
    assert len(pg_1)

    pg_2 = get_or_create_playground(
        dr_endpoint,
        dr_token,
        "pytest playground #1",
        use_case=use_case,
    )
    assert pg_1 == pg_2

    pg_3 = get_or_create_playground(
        dr_endpoint,
        dr_token,
        "pytest playground #2",
        use_case=use_case,
    )
    assert pg_1 != pg_3
