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

import datarobot as dr
from datarobotx.idp.llm_blueprints import get_or_create_llm_blueprint
from datarobotx.idp.playgrounds import get_or_create_playground
from datarobotx.idp.use_cases import get_or_create_use_case


@pytest.fixture
def use_case(dr_endpoint, dr_token, cleanup_dr):
    with cleanup_dr("useCases/"):
        yield get_or_create_use_case(
            dr_endpoint,
            dr_token,
            "pytest use case",
        )


@pytest.fixture
def playground(dr_endpoint, dr_token, cleanup_dr, use_case):
    with cleanup_dr("genai/playgrounds/"):
        yield get_or_create_playground(
            dr_endpoint, dr_token, "pytest playground", use_case=use_case
        )


@pytest.fixture
def cleanup_env(cleanup_dr):
    with cleanup_dr("genai/llmBlueprints/"):
        yield


@pytest.fixture
def llm(dr_endpoint, dr_token):
    from datarobot.models.genai.llm import LLMDefinition

    dr.Client(endpoint=dr_endpoint, token=dr_token)
    return LLMDefinition.list(as_dict=False)[0].id


def test_get_or_create(dr_endpoint, dr_token, playground, cleanup_env, llm):
    bp_1 = get_or_create_llm_blueprint(
        dr_endpoint, dr_token, playground, "pytest llm blueprint #1", llm=llm
    )
    assert len(bp_1)

    bp_2 = get_or_create_llm_blueprint(
        dr_endpoint, dr_token, playground, "pytest llm blueprint #1", llm=llm
    )
    assert bp_1 == bp_2

    bp_3 = get_or_create_llm_blueprint(
        dr_endpoint, dr_token, playground, "pytest llm blueprint #2", llm=llm
    )
    assert bp_1 != bp_3
