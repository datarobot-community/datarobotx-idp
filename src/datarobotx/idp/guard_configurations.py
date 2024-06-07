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

from typing import Any, Dict, List, Literal, Optional, TypedDict, Union

import datarobot as dr
from datarobot.models.runtime_parameters import RuntimeParameterValue
from datarobot.utils import from_api, to_api, underscorize

from datarobotx.idp.common.hashing import get_hash
from datarobotx.idp.custom_model_versions import (
    _get_or_create as _get_or_create_custom_model_version,
)
from datarobotx.idp.custom_model_versions import (
    _unsafe_get_or_create_custom_model_version_from_previous,
)


class Condition(TypedDict):
    """A condition for a guard configuration."""

    comparand: float
    comparator: Literal[
        "greaterThan",
        "lessThan",
        "equals",
        "notEquals",
        "is",
        "isNot",
        "matches",
        "doesNotMatch",
        "contains",
        "doesNotContain",
    ]


class Intervention(TypedDict):
    """An intervention for a guard configuration."""

    action: Literal["report", "block"]
    conditions: List[Condition]
    message: str
    send_notification: bool


_keys_to_remove = [
    "createdAt",
    "creatorId",
    "entityId",
    "entityType",
    "orgId",
    "allowedStages",
    "additionalConfig",
    "productionOnly",
    "id",
    "isDeployed",
]


def _clean_guard_configurations(
    guard_config: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    cleaned_config = []
    for config in guard_config:
        new_config = {}
        for k, v in config.items():
            if k in [underscorize(key) for key in _keys_to_remove]:
                continue
            if v is not None:
                new_config[k] = v
        cleaned_config.append(new_config)
    return cleaned_config


def _get_latest_model_version_id(endpoint: str, token: str, custom_model_id: str) -> str:
    custom_model = dr.CustomInferenceModel.get(custom_model_id)  # type: ignore
    if not custom_model.latest_version:
        raise ValueError("Custom model has no versions")

    latest_version = custom_model.latest_version
    return str(latest_version.id)


def _get_unfrozen_model_version_id(
    endpoint: str,
    token: str,
    custom_model_id: str,
    latest_version_id: str,
    **kwargs: Any,
) -> str:
    latest_version = dr.CustomModelVersion.get(custom_model_id, latest_version_id)  # type: ignore

    if latest_version.is_frozen:
        base_environment_id = latest_version.base_environment_id
        latest_version_id = _unsafe_get_or_create_custom_model_version_from_previous(
            endpoint=endpoint,
            token=token,
            custom_model_id=custom_model_id,
            base_environment_id=base_environment_id,
            **kwargs,
        )
    else:
        latest_version_id = latest_version.id
    return latest_version_id


def _get_current_guard_configurations(
    endpoint: str, token: str, latest_version_id: str
) -> List[Dict[str, Any]]:
    client = dr.Client(endpoint=endpoint, token=token)  # type: ignore
    guard_config = client.get(
        "guardConfigurations/",
        params={"entityId": latest_version_id, "entityType": "customModelVersion"},
    ).json()["data"]

    guard_config = [from_api(config) for config in guard_config]
    return guard_config  # type: ignore[no-any-return]


def _get_selected_guard_template(
    endpoint: str, token: str, guard_config_template_name: str
) -> Dict[str, Any]:
    client = dr.Client(endpoint=endpoint, token=token)  # type: ignore
    # get all guard templates
    guard_templates = client.get("guardTemplates/").json()["data"]
    guard_templates = [
        from_api(gt) for gt in guard_templates if gt["name"] == guard_config_template_name
    ]

    if not guard_templates:
        raise ValueError(f"Guard template {guard_config_template_name} not found")

    selected_template = guard_templates[0]
    return selected_template  # type: ignore[no-any-return]


def _assemble_guard_config(
    guard_name: str,
    guard_token: str,
    selected_template: Dict[str, Any],
    guard_config_template_settings: Dict[str, Any],
    stages: List[Union[Literal["prompt"], Literal["response"]]],
    intervention: Intervention,
    description: Optional[str] = None,
) -> Dict[str, Any]:
    # Check if the selected guard template supports the given stages
    if not all([stage in selected_template["allowed_stages"] for stage in stages]):
        raise ValueError(
            f"Guard template {selected_template['name']} does not support stages {stages}"
        )
    # Assemble the guard configuration with all expected fields
    cleaned_guard_template = _clean_guard_configurations([selected_template])[0]

    cleaned_guard_template[
        "description"
    ] = f"{description or cleaned_guard_template['description']} [{guard_token}]"
    cleaned_guard_template["stages"] = [stage for stage in stages]
    cleaned_guard_template["intervention"] = intervention
    cleaned_guard_template.update(guard_config_template_settings)
    cleaned_guard_template["name"] = guard_name
    return cleaned_guard_template


def _ensure_guard_config_from_template(  # noqa: PLR0913
    endpoint: str,
    token: str,
    custom_model_id: str,
    guard_config_template_name: str,
    guard_config_template_settings: Dict[str, Any],
    stages: List[Union[Literal["prompt"], Literal["response"]]],
    intervention: Intervention,
    replace: bool = False,
    **kwargs: Any,
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
    client = dr.Client(endpoint=endpoint, token=token)  # type: ignore

    guard_token = get_hash(
        custom_model_id,
        guard_config_template_name,
        stages,
        intervention,
        guard_config_template_settings,
        **kwargs,
    )

    guard_name = f"{kwargs.get('name') or guard_config_template_name}"

    latest_version_id = _get_latest_model_version_id(endpoint, token, custom_model_id)

    current_guard_config = _get_current_guard_configurations(endpoint, token, latest_version_id)

    # check if the guard configuration already exists and return early
    if current_guard_config:
        for config in current_guard_config:
            if guard_token in config.get("description", ""):
                return str(latest_version_id)

    cleaned_guard_config = _clean_guard_configurations(current_guard_config)

    if replace:
        # delete the existing guard configuration if it exists
        cleaned_guard_config = [
            config for config in cleaned_guard_config if guard_name != config["name"]
        ]

    selected_template = _get_selected_guard_template(endpoint, token, guard_config_template_name)

    # Assemble the guard configuration with all expected fields
    assembled_guard_template = _assemble_guard_config(
        guard_name=guard_name,
        guard_token=guard_token,
        selected_template=selected_template,
        guard_config_template_settings=guard_config_template_settings,
        stages=stages,
        intervention=intervention,
        description=kwargs.get("description"),
    )

    # append previous guard configurations
    cleaned_guard_config.append(assembled_guard_template)

    _ = _get_unfrozen_model_version_id(
        endpoint, token, custom_model_id, latest_version_id, **kwargs
    )

    res = client.post(
        "guardConfigurations/toNewCustomModelVersion/",
        json={
            "data": [to_api(config) for config in cleaned_guard_config],
            "customModelId": custom_model_id,
        },
    )

    return str(res.json()["customModelVersionId"])


def _unsafe_get_or_create_custom_model_version_with_guard_config(  # noqa: PLR0913
    endpoint: str,
    token: str,
    custom_model_id: str,
    guard_config_template_name: str,
    guard_config_template_settings: Dict[str, Any],
    stages: List[Union[Literal["prompt"], Literal["response"]]],
    intervention: Intervention,
    **kwargs: Any,
) -> str:
    """Add a guard configuration to a custom model version.

    Unsafe here means that idempotency is not guaranteed! Running this function multiple times with different
    arguments can lead to undefined behaviour.

    If a guard configuration with the same settings already exists, it will not be created.

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
    return _ensure_guard_config_from_template(
        endpoint=endpoint,
        token=token,
        custom_model_id=custom_model_id,
        guard_config_template_name=guard_config_template_name,
        guard_config_template_settings=guard_config_template_settings,
        stages=stages,
        intervention=intervention,
        replace=False,
        **kwargs,
    )


def _unsafe_get_update_or_create_custom_model_version_with_guard_config(  # noqa: PLR0913
    endpoint: str,
    token: str,
    custom_model_id: str,
    guard_config_template_name: str,
    guard_config_template_settings: Dict[str, Any],
    stages: List[Union[Literal["prompt"], Literal["response"]]],
    intervention: Intervention,
    **kwargs: Any,
) -> str:
    """Add or replace a guard configuration to a custom model version, reusing the previous version.

    Unsafe here means that idempotency is not guaranteed! Running this function multiple times with different
    arguments can lead to undefined behaviour.

    If a guard configuration with the same settings already exists, no new guard configuration will be created.
    Replacement is based on the name of the guard configuration or the given name.
    If the guard configuration with the same name but different settings exists, it will be replaced.

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
    return _ensure_guard_config_from_template(
        endpoint=endpoint,
        token=token,
        custom_model_id=custom_model_id,
        guard_config_template_name=guard_config_template_name,
        guard_config_template_settings=guard_config_template_settings,
        stages=stages,
        intervention=intervention,
        replace=True,
        **kwargs,
    )


def get_or_create_custom_model_version_with_guard_config(
    endpoint: str,
    token: str,
    custom_model_id: str,
    guard_configs: List[Dict[str, Any]],
    runtime_parameter_values: Optional[List[Union[RuntimeParameterValue, Dict[str, Any]]]] = None,
    **kwargs: Any,
) -> str:
    """Get or create a custom model version with requested parameters.

    Creates a new custom model version from scratch, and optionally adds guard templates.

    Parameters
    ----------
    custom_model_id : str
        The ID of the custom model to create the guard configuration for.
    guard_configs : Optional[List[Dict[str, Any]]
        A list of configuration dictionaries for each guard template to add.
        Example:
        [
            {
                "guard_config_template_name": "Prompt Injection",
                "guard_config_template_settings": {
                    "deploymentId": "some_deployment_id",
                },
                "stages": ["prompt", "response"],
                "intervention": {
                    "action": "block",
                    "conditions": [
                        {
                            "comparand": 0.5,
                            "comparator": "greaterThan",
                        }
                    ],
                },
            },
        ...
        ]
    runtime_parameter_values : Optional[List[Union[RuntimeParameterValue, Dict[str, Any]]]], optional
        The values for the runtime parameters, by default None

    Returns
    -------
    str
        The ID of the custom model version with the requested parameters.

    Notes
    -----
    Records a checksum in the model version description field to allow future calls to this function
    to validate whether a desired model version already exists
    """
    _ = _get_or_create_custom_model_version(
        from_previous=False,
        endpoint=endpoint,
        token=token,
        custom_model_id=custom_model_id,
        runtime_parameter_values=runtime_parameter_values,
        args_to_hash=guard_configs,
        **kwargs,
    )
    for guard_config in guard_configs:
        new_version = _unsafe_get_update_or_create_custom_model_version_with_guard_config(
            endpoint=endpoint, token=token, custom_model_id=custom_model_id, **guard_config
        )
    return new_version
