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
from datarobotx.idp.custom_model_versions import (  # type: ignore
    get_or_create_custom_model_version,
    get_or_create_custom_model_version_with_guard_config,
)
from datarobotx.idp.custom_models import get_or_create_custom_model  # type: ignore
from datarobotx.idp.deployments import (  # type: ignore
    get_or_create_deployment_from_registered_model_version,
)
from datarobotx.idp.llm_blueprints import (  # type: ignore
    get_or_create_llm_blueprint,
    get_or_register_llm_blueprint_custom_model_version,
)
from datarobotx.idp.playgrounds import get_or_create_playground  # type: ignore
from datarobotx.idp.use_cases import get_or_create_use_case  # type: ignore


@pytest.fixture
def custom_model_id(dr_endpoint: str, dr_token: str, cleanup_dr: Any) -> Generator[str, None, None]:
    with cleanup_dr("customModels"):
        custom_model = get_or_create_custom_model(
            endpoint=dr_endpoint,
            token=dr_token,
            name="pytest Custom Model",
            target_type="TextGeneration",
            target_name="text",
        )
        yield custom_model


@pytest.fixture
def base_environment_id() -> str:
    return "65f9b27eab986d30d4c64268"


@pytest.fixture
def custom_model_version_id(
    dr_endpoint: str, dr_token: str, custom_model_id: str, cleanup_dr: Any, base_environment_id: str
) -> Generator[str, None, None]:
    with cleanup_dr("customModels"):
        custom_model = get_or_create_custom_model_version(
            endpoint=dr_endpoint,
            token=dr_token,
            custom_model_id=custom_model_id,
            base_environment_id=base_environment_id,
        )
        yield custom_model


@pytest.fixture
def guard_deployment_toxicity(
    dr_endpoint: str, dr_token: str, default_prediction_server_id: str, cleanup_dr: Any
) -> Generator[str, None, None]:
    registered_model_name = "Toxicity Classifier"
    deployment_name = "Toxicity Deployment pytest"

    with cleanup_dr("deployments"):
        for version in dr.RegisteredModel.list(search=registered_model_name)[0].list_versions():  # type: ignore
            if version.build_status == "failed":
                continue
            registered_model_vers_id = version.id
            break
        deployment = get_or_create_deployment_from_registered_model_version(
            endpoint=dr_endpoint,
            token=dr_token,
            registered_model_version_id=registered_model_vers_id,
            label=deployment_name,
            default_prediction_server_id=default_prediction_server_id,
        )
        yield deployment


@pytest.fixture
def guard_template_name_toxicity() -> str:
    return "Toxicity"


@pytest.fixture
def intervention() -> dict[str, Any]:
    return {
        "action": "block",
        "message": "This is a block message.",
        "conditions": [{"comparand": 0.5, "comparator": "greaterThan"}],
        "send_notification": True,
    }


@pytest.fixture
def intervention_2() -> dict[str, Any]:
    return {
        "action": "block",
        "message": "This is a different block message.",
        "conditions": [{"comparand": 0.5, "comparator": "greaterThan"}],
        "send_notification": True,
    }


@pytest.fixture
def guard_deployment_prompt_injection(
    dr_endpoint: str, dr_token: str, default_prediction_server_id: str, cleanup_dr: Any
) -> Generator[str, None, None]:
    registered_model_name = "Prompt Injection Classifier"
    deployment_name = "Prompt Injection Deployment pytest"

    with cleanup_dr("deployments"):
        for version in dr.RegisteredModel.list(search=registered_model_name)[0].list_versions():  # type: ignore
            if version.build_status == "failed":
                continue
            registered_model_vers_id = version.id
            break
        deployment = get_or_create_deployment_from_registered_model_version(
            endpoint=dr_endpoint,
            token=dr_token,
            registered_model_version_id=registered_model_vers_id,
            label=deployment_name,
            default_prediction_server_id=default_prediction_server_id,
        )
        yield deployment


@pytest.fixture
def guard_template_name_prompt_injection() -> str:
    return "Prompt Injection"


def _get_guard_configs(custom_model_version_id: str) -> Any:
    client = dr.client.get_client()
    return client.get(
        "guardConfigurations/",
        params={"entityId": custom_model_version_id, "entityType": "customModelVersion"},
    ).json()["data"]


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


def test_get_or_create_custom_model_version_with_guard_config(
    dr_endpoint: str,
    dr_token: str,
    custom_model_id: str,
    guard_template_name_toxicity: str,
    guard_deployment_toxicity: str,
    guard_template_name_prompt_injection: str,
    guard_deployment_prompt_injection: str,
    custom_model_version_id: str,
    base_environment_id: str,
    intervention: dict[str, Any],
    intervention_2: dict[str, Any],
) -> None:
    # Call the function
    assert custom_model_version_id
    model_version_1 = get_or_create_custom_model_version_with_guard_config(
        endpoint=dr_endpoint,
        token=dr_token,
        base_environment_id=base_environment_id,
        custom_model_id=custom_model_id,
        guard_configs=[
            {
                "guard_config_template_name": guard_template_name_toxicity,
                "guard_config_template_settings": {
                    "deploymentId": guard_deployment_toxicity,
                },
                "stages": ["prompt"],
                "intervention": intervention,
            },
            {
                "guard_config_template_name": guard_template_name_prompt_injection,
                "guard_config_template_settings": {
                    "deploymentId": guard_deployment_prompt_injection,
                },
                "stages": ["prompt"],
                "intervention": intervention,
            },
        ],
    )

    assert model_version_1

    guard_config = _get_guard_configs(model_version_1)
    assert len(guard_config) == 2
    toxicity_guard_config = [
        gc for gc in guard_config if guard_template_name_toxicity in gc["name"]
    ][0]
    assert toxicity_guard_config["intervention"]["message"] == intervention["message"]
    injection_guard_config = [
        gc for gc in guard_config if guard_template_name_prompt_injection in gc["name"]
    ][0]
    assert injection_guard_config["intervention"]["message"] == intervention["message"]

    model_version_2 = get_or_create_custom_model_version_with_guard_config(
        endpoint=dr_endpoint,
        token=dr_token,
        custom_model_id=custom_model_id,
        base_environment_id=base_environment_id,
        guard_configs=[
            {
                "guard_config_template_name": guard_template_name_toxicity,
                "guard_config_template_settings": {
                    "deploymentId": guard_deployment_toxicity,
                },
                "stages": ["prompt"],
                "intervention": intervention,
            },
            {
                "guard_config_template_name": guard_template_name_prompt_injection,
                "guard_config_template_settings": {
                    "deploymentId": guard_deployment_prompt_injection,
                },
                "stages": ["prompt"],
                "intervention": intervention,
            },
        ],
    )

    assert model_version_2 == model_version_1

    model_version_3 = get_or_create_custom_model_version_with_guard_config(
        endpoint=dr_endpoint,
        token=dr_token,
        base_environment_id=base_environment_id,
        custom_model_id=custom_model_id,
        guard_configs=[
            {
                "guard_config_template_name": guard_template_name_toxicity,
                "guard_config_template_settings": {
                    "deploymentId": guard_deployment_toxicity,
                },
                "stages": ["prompt"],
                "intervention": intervention,
            },
            {
                "guard_config_template_name": guard_template_name_prompt_injection,
                "guard_config_template_settings": {
                    "deploymentId": guard_deployment_prompt_injection,
                },
                "stages": ["prompt"],
                "intervention": intervention_2,
            },
        ],
    )
    assert model_version_3 != model_version_1
    guard_config_2 = _get_guard_configs(model_version_3)
    assert len(guard_config_2) == 2
    toxicity_guard_config_2 = [
        gc for gc in guard_config_2 if guard_template_name_toxicity in gc["name"]
    ][0]
    assert toxicity_guard_config_2["intervention"]["message"] == intervention["message"]
    injection_guard_config_2 = [
        gc for gc in guard_config_2 if guard_template_name_prompt_injection in gc["name"]
    ][0]
    assert injection_guard_config_2["intervention"]["message"] == intervention_2["message"]


def test_get_or_register_llm_blueprint_custom_model_version(
    dr_endpoint: str,
    dr_token: str,
    playground: str,
    llm: str,
    cleanup_dr: Any,
    guard_deployment_prompt_injection: str,
    guard_deployment_toxicity: str,
    guard_template_name_toxicity: str,
    guard_template_name_prompt_injection: str,
    intervention: dict[str, Any],
    intervention_2: dict[str, Any],
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
            endpoint=dr_endpoint,
            token=dr_token,
            llm_blueprint_id=bp_1,
            guard_configs=[
                {
                    "guard_config_template_name": guard_template_name_toxicity,
                    "guard_config_template_settings": {
                        "deploymentId": guard_deployment_toxicity,
                    },
                    "stages": ["prompt"],
                    "intervention": intervention,
                },
                {
                    "guard_config_template_name": guard_template_name_prompt_injection,
                    "guard_config_template_settings": {
                        "deploymentId": guard_deployment_prompt_injection,
                    },
                    "stages": ["prompt"],
                    "intervention": intervention,
                },
            ],
        )
        assert len(cm_1)
        assert len(cmv_1)

        cm_2, cmv_2 = get_or_register_llm_blueprint_custom_model_version(
            endpoint=dr_endpoint,
            token=dr_token,
            llm_blueprint_id=bp_1,
            guard_configs=[
                {
                    "guard_config_template_name": guard_template_name_toxicity,
                    "guard_config_template_settings": {
                        "deploymentId": guard_deployment_toxicity,
                    },
                    "stages": ["prompt"],
                    "intervention": intervention,
                },
                {
                    "guard_config_template_name": guard_template_name_prompt_injection,
                    "guard_config_template_settings": {
                        "deploymentId": guard_deployment_prompt_injection,
                    },
                    "stages": ["prompt"],
                    "intervention": intervention,
                },
            ],
        )
        assert cm_1 == cm_2
        assert cmv_1 == cmv_2

        cm_3, cmv_3 = get_or_register_llm_blueprint_custom_model_version(
            endpoint=dr_endpoint,
            token=dr_token,
            llm_blueprint_id=bp_2,
            guard_configs=[
                {
                    "guard_config_template_name": guard_template_name_toxicity,
                    "guard_config_template_settings": {
                        "deploymentId": guard_deployment_toxicity,
                    },
                    "stages": ["prompt"],
                    "intervention": intervention,
                },
                {
                    "guard_config_template_name": guard_template_name_prompt_injection,
                    "guard_config_template_settings": {
                        "deploymentId": guard_deployment_prompt_injection,
                    },
                    "stages": ["prompt"],
                    "intervention": intervention_2,
                },
            ],
        )
        assert cm_1 != cm_3
        assert cmv_1 != cmv_3


# def test_unsafe_get_or_create_custom_model_version_with_guard_config(
#     dr_endpoint: str,
#     dr_token: str,
#     custom_model_id: str,
#     guard_template_name_toxicity: str,
#     guard_deployment_toxicity: str,
#     guard_template_name_prompt_injection: str,
#     guard_deployment_prompt_injection: str,
#     intervention: Dict[str, Any],
# ) -> None:
#     latest_version = unsafe_get_or_create_custom_model_version_with_guard_config(
#         endpoint=dr_endpoint,
#         token=dr_token,
#         custom_model_id=custom_model_id,
#         guard_config_template_name=guard_template_name_toxicity,
#         guard_config_template_settings={
#             "deploymentId": guard_deployment_toxicity,
#         },
#         stages=["prompt"],
#         intervention=intervention,
#         name="pytest Toxicity Guard Configuration",
#     )

#     # Check the result
#     guard_config = _get_guard_configs(latest_version)
#     assert guard_config
#     assert any(["Toxicity" in gc["name"] for gc in guard_config])

#     latest_version_2 = unsafe_get_or_create_custom_model_version_with_guard_config(
#         endpoint=dr_endpoint,
#         token=dr_token,
#         custom_model_id=custom_model_id,
#         guard_config_template_name=guard_template_name_toxicity,
#         guard_config_template_settings={
#             "deploymentId": guard_deployment_toxicity,
#         },
#         stages=["prompt"],
#         intervention=intervention,
#         name="pytest Toxicity Guard Configuration",
#     )

#     assert latest_version == latest_version_2

#     latest_version_3 = unsafe_get_or_create_custom_model_version_with_guard_config(
#         endpoint=dr_endpoint,
#         token=dr_token,
#         custom_model_id=custom_model_id,
#         guard_config_template_name=guard_template_name_prompt_injection,
#         guard_config_template_settings={
#             "deploymentId": guard_deployment_prompt_injection,
#         },
#         stages=["prompt"],
#         intervention=intervention,
#         name="pytest Prompt Injection Guard Configuration",
#     )

#     # Check the result
#     guard_config = _get_guard_configs(latest_version_3)
#     assert guard_config
#     assert any(["Prompt Injection" in gc["name"] for gc in guard_config]) and any(
#         ["Toxicity" in gc["name"] for gc in guard_config]
#     )

#     assert latest_version != latest_version_3
