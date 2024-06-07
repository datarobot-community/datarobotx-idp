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


from pathlib import Path
from typing import Any

import datarobot as dr
from datarobot.enums import EXECUTION_ENVIRONMENT_VERSION_BUILD_STATUS

from datarobotx.idp.common.hashing import get_hash


def _find_existing_environment_version(
    execution_environment_id: str, env_version_token: str
) -> str:
    for env in dr.ExecutionEnvironmentVersion.list(execution_environment_id):  # type: ignore
        if env.description and (
            env_version_token in env.description
            and env.build_status == EXECUTION_ENVIRONMENT_VERSION_BUILD_STATUS.SUCCESS
        ):
            return str(env.id)
    raise KeyError("No matching environment version found")


def get_or_create_execution_environment_version(
    endpoint: str,
    token: str,
    execution_environment_id: str,
    docker_context_path: str,
    **kwargs: Any,
) -> str:
    """Get or create an execution environment version with requested parameters.

    Notes
    -----
    Records a checksum in the environment version description field to allow future calls to this
    function to validate whether a desired environment version already exists
    """
    dr.Client(token=token, endpoint=endpoint)  # type: ignore[attr-defined]
    env_version_token = get_hash(Path(docker_context_path), execution_environment_id, **kwargs)

    try:
        return _find_existing_environment_version(execution_environment_id, env_version_token)
    except KeyError as exc:
        kwargs["description"] = kwargs.get("description", "") + f"\nChecksum: {env_version_token}"
        env_version = dr.ExecutionEnvironmentVersion.create(  # type: ignore
            execution_environment_id, docker_context_path, max_wait=45 * 60, **kwargs
        )
        if env_version.build_status != EXECUTION_ENVIRONMENT_VERSION_BUILD_STATUS.SUCCESS:
            raise ValueError(
                f"Custom environment {env_version.id} was not successful; please see the build logs in DR"
            ) from exc
        return str(env_version.id)
