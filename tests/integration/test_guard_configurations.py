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

# mypy: disable-error-code="attr-defined"
# pyright: reportPrivateImportUsage=false

from typing import Any, Generator

import pytest

import datarobot as dr
from datarobotx.idp.custom_model_versions import get_or_create_custom_model_version  # type: ignore
from datarobotx.idp.custom_models import get_or_create_custom_model  # type: ignore
from datarobotx.idp.deployments import (  # type: ignore
    get_or_create_deployment_from_registered_model_version,
)
from datarobotx.idp.guard_configurations import (  # type: ignore
    get_or_create_guard_config_to_custom_model_version,
    get_update_or_create_guard_config_to_custom_model_version,
)


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
def custom_model_version_id(
    dr_endpoint: str, dr_token: str, custom_model_id: str, cleanup_dr: Any
) -> Generator[str, None, None]:
    with cleanup_dr("customModels"):
        custom_model = get_or_create_custom_model_version(
            endpoint=dr_endpoint,
            token=dr_token,
            custom_model_id=custom_model_id,
            base_environment_id="65f9b27eab986d30d4c64268",
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

def _get_guard_configs(custom_model_version_id: str)-> Any:
    client = dr.client.get_client()
    return client.get(
        "guardConfigurations/",
        params={"entityId": custom_model_version_id, "entityType": "customModelVersion"},
    ).json()["data"]

def test_get_or_create_guard_config_to_custom_model_version(
    dr_endpoint: str,
    dr_token: str,
    custom_model_id: str,
    guard_template_name_toxicity: str,
    guard_deployment_toxicity: str,
    guard_template_name_prompt_injection: str,
    guard_deployment_prompt_injection: str,
    custom_model_version_id: str,
    intervention: dict[str, Any],
) -> None:
    # Call the function
    assert custom_model_version_id
    latest_version = get_or_create_guard_config_to_custom_model_version(
        endpoint=dr_endpoint,
        token=dr_token,
        custom_model_id=custom_model_id,
        guard_config_template_name=guard_template_name_toxicity,
        guard_config_template_settings={
            "deploymentId": guard_deployment_toxicity,
        },
        stages=["prompt"],
        intervention=intervention,
        name="pytest Toxicity Guard Configuration",
    )

    # Check the result
    guard_config = _get_guard_configs(latest_version)
    assert guard_config
    assert any(["Toxicity" in gc["name"] for gc in guard_config])

    latest_version_2 = get_or_create_guard_config_to_custom_model_version(
        endpoint=dr_endpoint,
        token=dr_token,
        custom_model_id=custom_model_id,
        guard_config_template_name=guard_template_name_toxicity,
        guard_config_template_settings={
            "deploymentId": guard_deployment_toxicity,
        },
        stages=["prompt"],
        intervention=intervention,
        name="pytest Toxicity Guard Configuration",
    )

    assert latest_version == latest_version_2

    latest_version_3 = get_or_create_guard_config_to_custom_model_version(
        endpoint=dr_endpoint,
        token=dr_token,
        custom_model_id=custom_model_id,
        guard_config_template_name=guard_template_name_prompt_injection,
        guard_config_template_settings={
            "deploymentId": guard_deployment_prompt_injection,
        },
        stages=["prompt"],
        intervention=intervention,
        name="pytest Prompt Injection Guard Configuration",
    )

    # Check the result
    guard_config = _get_guard_configs(latest_version_3)
    assert guard_config
    assert any(["Prompt Injection" in gc["name"] for gc in guard_config]) and any(
        ["Toxicity" in gc["name"] for gc in guard_config]
    )

    assert latest_version != latest_version_3


def test_get_update_or_create_guard_config_to_custom_model_version(
    dr_endpoint: str,
    dr_token: str,
    custom_model_id: str,
    guard_template_name_toxicity: str,
    guard_deployment_toxicity: str,
    guard_template_name_prompt_injection: str,
    guard_deployment_prompt_injection: str,
    custom_model_version_id: str,
    intervention: dict[str, Any],
    intervention_2: dict[str, Any],
) -> None:
    # Call the function
    assert custom_model_version_id
    latest_version = get_update_or_create_guard_config_to_custom_model_version(
        endpoint=dr_endpoint,
        token=dr_token,
        custom_model_id=custom_model_id,
        guard_config_template_name=guard_template_name_toxicity,
        guard_config_template_settings={
            "deploymentId": guard_deployment_toxicity,
        },
        stages=["prompt"],
        intervention=intervention,
        name="pytest Toxicity Guard Configuration",
    )

    # Check the result
    guard_config = _get_guard_configs(latest_version)
    assert guard_config
    assert any([guard_template_name_toxicity in gc["name"] for gc in guard_config])

    latest_version_2 = get_update_or_create_guard_config_to_custom_model_version(
        endpoint=dr_endpoint,
        token=dr_token,
        custom_model_id=custom_model_id,
        guard_config_template_name=guard_template_name_toxicity,
        guard_config_template_settings={
            "deploymentId": guard_deployment_toxicity,
        },
        stages=["prompt"],
        intervention=intervention,
        name="pytest Toxicity Guard Configuration",
    )

    assert latest_version == latest_version_2

    # ensure guard_template_name_toxicity appears only once
    guard_config = _get_guard_configs(latest_version_2)
    assert len([gc for gc in guard_config if guard_template_name_toxicity in gc["name"]]) == 1

    latest_version_3 = get_update_or_create_guard_config_to_custom_model_version(
        endpoint=dr_endpoint,
        token=dr_token,
        custom_model_id=custom_model_id,
        guard_config_template_name=guard_template_name_prompt_injection,
        guard_config_template_settings={
            "deploymentId": guard_deployment_prompt_injection,
        },
        stages=["prompt"],
        intervention=intervention,
        name="pytest Prompt Injection Guard Configuration",
    )

    # Check the result
    guard_config = _get_guard_configs(latest_version_3)
    assert guard_config
    assert any([guard_template_name_prompt_injection in gc["name"] for gc in guard_config]) and any(
        [guard_template_name_toxicity in gc["name"] for gc in guard_config]
    )

    assert latest_version != latest_version_3

    latest_version_4 = get_update_or_create_guard_config_to_custom_model_version(
        endpoint=dr_endpoint,
        token=dr_token,
        custom_model_id=custom_model_id,
        guard_config_template_name=guard_template_name_toxicity,
        guard_config_template_settings={
            "deploymentId": guard_deployment_toxicity,
        },
        stages=["prompt"],
        intervention=intervention,
        name="pytest Toxicity Guard Configuration",
    )

    assert latest_version_3 == latest_version_4
    # ensure guard_template_name_toxicity appears only once
    guard_config = _get_guard_configs(latest_version_4)
    assert len([gc for gc in guard_config if guard_template_name_toxicity in gc["name"]]) == 1

    latest_version_5 = get_update_or_create_guard_config_to_custom_model_version(
        endpoint=dr_endpoint,
        token=dr_token,
        custom_model_id=custom_model_id,
        guard_config_template_name=guard_template_name_toxicity,
        guard_config_template_settings={
            "deploymentId": guard_deployment_toxicity,
        },
        stages=["prompt"],
        intervention=intervention_2,
        name="pytest Toxicity Guard Configuration",
    )

    assert latest_version_4 != latest_version_5
    # ensure guard_template_name_toxicity appears only once
    guard_config = _get_guard_configs(latest_version_5)
    assert len([gc for gc in guard_config if guard_template_name_toxicity in gc["name"]]) == 1