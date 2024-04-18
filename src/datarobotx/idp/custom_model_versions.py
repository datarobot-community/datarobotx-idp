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

# mypy: disable-error-code="attr-defined"
# pyright: reportPrivateImportUsage=false
import json
import pathlib
from pathlib import Path
from typing import Any, Dict, List, Optional

import datarobot as dr
from datarobot.utils import camelize, to_api

from datarobotx.idp.common.hashing import get_hash


def _find_existing_custom_model_version(custom_model_id: str, model_version_token: str) -> str:
    for version in dr.CustomModelVersion.list(custom_model_id):
        if version.description is not None and model_version_token in version.description:
            return str(version.id)
    raise KeyError("No matching model version found")


def _patch_custom_model_version(
    endpoint: str,
    token: str,
    custom_model_id: str,
    base_environment_id: str,
    **kwargs: Any,
) -> str:
    url = f"customModels/{custom_model_id}/versions/"
    kwargs["base_environment_id"] = base_environment_id
    body = {camelize(k): v for k, v in kwargs.items()}
    resp = dr.Client(token=token, endpoint=endpoint).patch(url=url, json=body)
    return str(resp.json()["id"])


def _ensure_dependency_build(custom_model_id: str, custom_model_version_id: str) -> None:
    try:
        assert (
            dr.CustomModelVersionDependencyBuild.get_build_info(
                custom_model_id, custom_model_version_id
            ).build_status
            == "success"
        )
    except (dr.errors.ClientError, AssertionError):
        dr.CustomModelVersionDependencyBuild.start_build(
            custom_model_id, custom_model_version_id, max_wait=20 * 60
        )


def get_or_create_custom_model_version(
    endpoint: str,
    token: str,
    custom_model_id: str,
    base_environment_id: str,
    folder_path: str,
    runtime_parameter_values: Optional[List[Dict[str, Any]]] = None,
    **kwargs: Any,
) -> str:
    """Get or create a custom model version with requested parameters.

    Notes
    -----
    Records a checksum in the model version description field to allow future calls to this
    function to validate whether a desired model version already exists
    """
    dr.Client(token=token, endpoint=endpoint)
    model_version_token = get_hash(
        Path(folder_path),
        custom_model_id,
        base_environment_id,
        runtime_parameter_values=runtime_parameter_values,
        **kwargs,
    )

    try:
        existing_version_id = _find_existing_custom_model_version(
            custom_model_id, model_version_token
        )
        if (pathlib.Path(folder_path) / "requirements.txt").exists():
            _ensure_dependency_build(custom_model_id, existing_version_id)
        return existing_version_id

    except KeyError:
        env_version = dr.CustomModelVersion.create_clean(
            custom_model_id,
            base_environment_id,
            folder_path=folder_path,
            max_wait=20 * 60,
            **kwargs,
        )
        if runtime_parameter_values is not None:
            env_version_id = _patch_custom_model_version(
                endpoint,
                token,
                custom_model_id,
                base_environment_id,
                is_major_update="false",
                runtime_parameter_values=json.dumps(
                    [to_api(param) for param in runtime_parameter_values]
                ),
            )
            env_version = dr.CustomModelVersion.get(custom_model_id, env_version_id)

        env_version.update(description=f"\nChecksum: {model_version_token}")  # pylint: disable=no-member

        if (pathlib.Path(folder_path) / "requirements.txt").exists():
            _ensure_dependency_build(custom_model_id, env_version.id)  # pylint: disable=no-member
        return str(env_version.id)  # pylint: disable=no-member
