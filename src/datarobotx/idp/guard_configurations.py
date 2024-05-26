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

from typing import Any, Dict, List, Optional

import datarobot as dr
from datarobot.errors import ClientError
from datarobot.utils import from_api, to_api

from datarobotx.idp.common.hashing import get_hash
from datarobotx.idp.custom_model_versions import (
    get_or_create_custom_model_version_from_previous,
)


def _clean_guard_configurations(guard_config: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    cleaned_config = []
    for config in guard_config:
        new_config = {}
        for k, v in config.items():
            if k in [
                "created_at",
                "creator_id",
                "entity_id",
                "entity_type",
                "org_id",
                "allowed_stages",
                "additional_config",
                "production_only",
                "id",
            ]:
                continue
            if v is not None:
                new_config[k] = v
        cleaned_config.append(new_config)
    return cleaned_config


def ensure_guard_config_from_template(  # noqa: PLR0913
    endpoint: str,
    token: str,
    custom_model_id: str,
    guard_config_template_name: str,
    guard_config_template_settings: Dict[str, Any],
    stages: List[str],
    intervention: Dict[str, Any],
    name: Optional[str] = None,
) -> str:
    """Ensure a guard configuration exists for a custom model version.

    Parameters
    ----------
    endpoint : str
        The DataRobot endpoint to connect to.
    token : str
        The DataRobot API token to use.
    custom_model_id : str
        The ID of the custom model to create the guard configuration for.
    guard_config_template_name : str
        The name of the guard configuration template to use.
    guard_config_template_settings : dict
        The settings for the guard configuration template.
    stages : Union[List[GuardStage], List[str]]
        The stages to apply the guard configuration to.
    intervention : Union[Intervention, Dict[str, Any]]
        The intervention to apply when the guard conditions are met.
    name : Optional[str], optional
        The name of the guard configuration, by default None

    Returns
    -------
    str
        The ID of the custom model version with the guard configuration.
    """
    if endpoint is not None and token is not None:
        client = dr.Client(endpoint=endpoint, token=token)  # type: ignore
    else:
        client = dr.client.get_client()

    guard_token = get_hash(
        custom_model_id,
        guard_config_template_name,
        stages,
        intervention,
        guard_config_template_settings,
        name,
    )

    guard_name = f"{name or guard_config_template_name} - [{guard_token}]"

    custom_model = dr.CustomInferenceModel.get(custom_model_id)  # type: ignore
    if not custom_model.latest_version:
        raise ValueError("Custom model has no versions")
    latest_version_id = custom_model.latest_version.id

    # we extract the base environment id from the latest version to be reused later
    base_environment_id = custom_model.latest_version.base_environment_id

    # get the current guard configurations
    guard_config = client.get(
        "guardConfigurations/",
        params={"entityId": latest_version_id, "entityType": "customModelVersion"},
    ).json()["data"]

    guard_config = [from_api(config) for config in guard_config]

    # check if the guard configuration already exists
    if guard_config:
        for config in guard_config:
            if config["name"] == guard_name:
                return str(latest_version_id)

    cleaned_guard_config = _clean_guard_configurations(guard_config)

    # get the guard templates
    guard_templates = client.get("guardTemplates/").json()["data"]
    guard_templates = [
        from_api(gt) for gt in guard_templates if gt["name"] == guard_config_template_name
    ]

    if not guard_templates:
        raise ValueError(f"Guard template {guard_config_template_name} not found")

    selected_template = guard_templates[0]
    if not all([stage in selected_template["allowed_stages"] for stage in stages]):
        raise ValueError(
            f"Guard template {guard_config_template_name} does not support stages {stages}"
        )

    # Assemble the guard configuration with all expected fields
    cleaned_guard_template = _clean_guard_configurations([selected_template])[0]
    cleaned_guard_template["stages"] = [stage for stage in stages]
    cleaned_guard_template["intervention"] = intervention
    cleaned_guard_template.update(guard_config_template_settings)
    cleaned_guard_template["name"] = guard_name

    # append previous guard configurations
    cleaned_guard_config.append(cleaned_guard_template)
    try:
        res = client.post(
            "guardConfigurations/toNewCustomModelVersion/",
            json={
                "data": [to_api(config) for config in cleaned_guard_config],
                "customModelId": custom_model_id,
            },
        )
    except ClientError:
        _ = get_or_create_custom_model_version_from_previous(
            endpoint=endpoint,
            token=token,
            custom_model_id=custom_model_id,
            base_environment_id=base_environment_id,
        )
        res = client.post(
            "guardConfigurations/toNewCustomModelVersion/",
            json={
                "data": [to_api(config) for config in cleaned_guard_config],
                "customModelId": custom_model_id,
            },
        )
    return str(res.json()["customModelVersionId"])
