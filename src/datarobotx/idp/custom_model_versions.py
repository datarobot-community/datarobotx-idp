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

import pathlib
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import datarobot as dr

from datarobotx.idp.common.hashing import get_hash

try:
    from datarobot.models.runtime_parameters import RuntimeParameterValue
except ImportError as e:
    raise ImportError(
        "datarobot>=3.4.0 is required for custom model versions with runtime parameters"
    ) from e


def _find_existing_custom_model_version(
    custom_model_id: str, model_version_token: str, from_previous: bool
) -> str:
    if from_previous:
        versions = []
        latest = dr.CustomInferenceModel.get(custom_model_id).latest_version  # type: ignore
        if latest is not None:
            versions.append(latest)
    else:
        versions = dr.CustomModelVersion.list(custom_model_id)  # type: ignore
    for version in versions:
        if version.description is not None and model_version_token in version.description:
            return str(version.id)
    raise KeyError("No matching model version found")


def _ensure_dependency_build(custom_model_id: str, custom_model_version_id: str) -> None:
    try:
        assert (
            dr.CustomModelVersionDependencyBuild.get_build_info(  # type: ignore
                custom_model_id, custom_model_version_id
            ).build_status
            == "success"
        )
    except (dr.errors.ClientError, AssertionError):
        dr.CustomModelVersionDependencyBuild.start_build(  # type: ignore
            custom_model_id, custom_model_version_id, max_wait=20 * 60
        )


def _has_requirements(folder_path: Optional[Any], files: Optional[List[Any]]) -> bool:
    if folder_path is not None and (pathlib.Path(folder_path) / "requirements.txt").exists():
        return True
    elif files is not None and any(
        [dest_path == "requirements.txt" for src_path, dest_path in files]
    ):
        return True
    else:
        return False


def _get_or_create(
    from_previous: bool,
    endpoint: str,
    token: str,
    custom_model_id: str,
    runtime_parameter_values: Optional[List[Union[RuntimeParameterValue, Dict[str, Any]]]] = None,
    args_to_hash: Optional[Any] = None,
    **kwargs: Any,
) -> str:
    if runtime_parameter_values is not None:
        runtime_parameter_values_objs = [
            RuntimeParameterValue(**d) for d in runtime_parameter_values if isinstance(d, dict)
        ]
    else:
        runtime_parameter_values_objs = None

    dr.Client(token=token, endpoint=endpoint)  # type: ignore
    folder_path = kwargs.pop("folder_path", None)
    max_wait = kwargs.pop("max_wait", dr.enums.DEFAULT_MAX_WAIT)
    model_version_token = get_hash(
        Path(folder_path) if folder_path is not None else None,
        custom_model_id,
        args_to_hash,
        runtime_parameter_values=runtime_parameter_values_objs,
        **kwargs,
    )

    try:
        existing_version_id = _find_existing_custom_model_version(
            custom_model_id, model_version_token, from_previous
        )
        if _has_requirements(folder_path, kwargs.get("files", None)):
            _ensure_dependency_build(custom_model_id, existing_version_id)
        return existing_version_id

    except KeyError:
        if not from_previous:
            create = dr.CustomModelVersion.create_clean  # type: ignore
        else:
            create = dr.CustomModelVersion.create_from_previous  # type: ignore
        env_version = create(
            custom_model_id,
            folder_path=folder_path,
            max_wait=max_wait,
            runtime_parameter_values=runtime_parameter_values_objs,
            **kwargs,
        )

        env_version.update(description=f"\nChecksum: {model_version_token}")

        if _has_requirements(folder_path, kwargs.get("files", None)):
            _ensure_dependency_build(custom_model_id, env_version.id)
        return str(env_version.id)


def _unsafe_get_or_create_custom_model_version_from_previous(
    endpoint: str,
    token: str,
    custom_model_id: str,
    runtime_parameter_values: Optional[List[Union[RuntimeParameterValue, Dict[str, Any]]]] = None,
    **kwargs: Any,
) -> str:
    """Get or create a custom model version from a previous version with requested parameters.

    Unsafe here means that idempotency is not guaranteed! Running this function multiple times with different arguments can lead to undefined behaviour.

    Notes
    -----
    Records a checksum in the model version description field to allow future calls to this
    function to validate whether a desired model version already exists
    """
    return _get_or_create(
        from_previous=True,
        endpoint=endpoint,
        token=token,
        custom_model_id=custom_model_id,
        runtime_parameter_values=runtime_parameter_values,
        **kwargs,
    )


def get_or_create_custom_model_version(
    endpoint: str,
    token: str,
    custom_model_id: str,
    runtime_parameter_values: Optional[List[Union[RuntimeParameterValue, Dict[str, Any]]]] = None,
    **kwargs: Any,
) -> str:
    """Get or create a custom model version with requested parameters.

    For a complete list of supported arguments, please refer to datarobot.CustomModelVersion.create_clean

    Parameters
    ----------
    custom_model_id : str
        The ID of the custom model to create a version for.
    runtime_parameter_values : Optional[List[Union[RuntimeParameterValue, Dict[str, Any]]]], optional
        The values for the runtime parameters. See datarobot.models.runtime_parameters.RuntimParameterValue for more information.
        Example runtime parameter values:
        [
            {
                "field_name": "OPENAI_API_KEY",
                "type": "credential",
                "value": <CREDENTIAL_ID>,
            },
            ...
        ]
     kwargs : Any, optional
        Additional arguments to pass to the model version creation call.

    Notes
    -----
    Records a checksum in the model version description field to allow future calls to this
    function to validate whether a desired model version already exists
    """
    return _get_or_create(
        from_previous=False,
        endpoint=endpoint,
        token=token,
        custom_model_id=custom_model_id,
        runtime_parameter_values=runtime_parameter_values,
        **kwargs,
    )
