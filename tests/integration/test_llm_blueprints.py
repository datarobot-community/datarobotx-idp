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

from typing import Any, Dict, Generator

import pytest

import datarobot as dr
from datarobotx.idp.llm_blueprints import (  # type: ignore
    get_or_create_llm_blueprint,
    get_or_register_llm_blueprint_custom_model_version,
)
from datarobotx.idp.playgrounds import get_or_create_playground  # type: ignore
from datarobotx.idp.use_cases import get_or_create_use_case  # type: ignore


@pytest.fixture(scope="class")
def use_case(dr_endpoint: str, dr_token: str, cleanup_dr: Any) -> Generator[str, None, None]:
    with cleanup_dr("useCases/"):
        yield get_or_create_use_case(
            dr_endpoint,
            dr_token,
            "pytest use case",
        )


@pytest.fixture(scope="class")
def playground(
    dr_endpoint: str, dr_token: str, cleanup_dr: Any, use_case: str
) -> Generator[str, None, None]:
    with cleanup_dr("genai/playgrounds/"):
        yield get_or_create_playground(
            dr_endpoint, dr_token, "pytest playground", use_case=use_case
        )


@pytest.fixture
def cleanup_env(cleanup_dr: Any, playground: str) -> Generator[None, None, None]:
    with cleanup_dr("genai/llmBlueprints/", params={"playgroundId": playground}):
        yield


@pytest.fixture(scope="class")
def llm(dr_endpoint: str, dr_token: str) -> str:
    from datarobot.models.genai.llm import LLMDefinition

    dr.Client(endpoint=dr_endpoint, token=dr_token)  # type: ignore[attr-defined]
    return str(LLMDefinition.list(as_dict=False)[0].id)  # type: ignore


@pytest.fixture
def llm_settings() -> Dict[str, Any]:
    return {"max_completion_length": 100}


class TestLLMBlueprints:
    def test_get_or_create(
        self,
        dr_endpoint: str,
        dr_token: str,
        playground: str,
        cleanup_env: Any,
        llm: str,
        llm_settings: Dict[str, Any],
    ) -> None:
        bp_1 = get_or_create_llm_blueprint(
            dr_endpoint,
            dr_token,
            playground,
            "pytest llm blueprint #1",
            llm=llm,
            llm_settings=llm_settings,
        )
        assert len(bp_1)

        bp_2 = get_or_create_llm_blueprint(
            dr_endpoint,
            dr_token,
            playground,
            "pytest llm blueprint #1",
            llm=llm,
            llm_settings=llm_settings,
        )
        assert bp_1 == bp_2

        bp_3 = get_or_create_llm_blueprint(
            dr_endpoint,
            dr_token,
            playground,
            "pytest llm blueprint #2",
            llm=llm,
            llm_settings=llm_settings,
        )
        assert bp_1 != bp_3

    def test_get_or_register_custom_model_version(
        self,
        dr_endpoint: str,
        dr_token: str,
        playground: str,
        cleanup_env: Any,
        cleanup_dr: Any,
        llm: str,
    ) -> None:
        bp_1 = get_or_create_llm_blueprint(
            endpoint=dr_endpoint,
            token=dr_token,
            playground=playground,
            name="pytest llm blueprint #1",
            llm=llm,
        )

        bp_2 = get_or_create_llm_blueprint(
            endpoint=dr_endpoint,
            token=dr_token,
            playground=playground,
            name="pytest llm blueprint #2",
            llm=llm,
        )

        with cleanup_dr("customModels/"):
            cm_1, cmv_1 = get_or_register_llm_blueprint_custom_model_version(
                endpoint=dr_endpoint, token=dr_token, llm_blueprint_id=bp_1
            )
            assert len(cm_1)
            assert len(cmv_1)

            cm_2, cmv_2 = get_or_register_llm_blueprint_custom_model_version(
                endpoint=dr_endpoint, token=dr_token, llm_blueprint_id=bp_1
            )
            assert cm_1 == cm_2
            assert cmv_1 == cmv_2

            cm_3, cmv_3 = get_or_register_llm_blueprint_custom_model_version(
                endpoint=dr_endpoint, token=dr_token, llm_blueprint_id=bp_2
            )
            assert cm_1 != cm_3
            assert cmv_1 != cmv_3
