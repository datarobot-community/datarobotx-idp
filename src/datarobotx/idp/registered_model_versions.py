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

import time
from typing import Any

import datarobot as dr
from datarobot import RegisteredModel, RegisteredModelVersion  # type: ignore[attr-defined]
from datarobot.models.model_registry.registered_model_version import (
    ExternalTarget,
)

from datarobotx.idp.common.hashing import get_hash


def _find_existing_registered_model(registered_model_name: str) -> RegisteredModel:
    for model in dr.RegisteredModel.list(search=registered_model_name):  # type: ignore[attr-defined]
        if model.name == registered_model_name:
            return model
    raise KeyError("No matching registered model found")


def _await_registered_model_build(
    registered_model_version: RegisteredModelVersion, max_wait: int = 600
) -> None:
    """Wait for a complete registered model version build status before returning.

    Cannot deploy to serverless prediction environments without this step.
    """
    if registered_model_version.build_status == "complete":
        return

    waited_secs = 0
    while True:
        rm = dr.RegisteredModel.get(registered_model_version.registered_model_id)  # type: ignore[attr-defined]
        rmv = rm.get_version(registered_model_version.id)
        if rmv.build_status == "complete":
            return
        elif rmv.build_status == "failed":
            msg = (
                f"Registered model version '{registered_model_version.id}' "
                f"for registered model '{registered_model_version.registered_model_id}' "
                "failed to build"
            )
            raise RuntimeError(msg)
        elif waited_secs > max_wait:
            msg = (
                "Timed out waiting for build for "
                f"registered model version '{registered_model_version.id}' "
                f"for registered model '{registered_model_version.registered_model_id}'"
            )
            raise TimeoutError(msg)
        time.sleep(3)
        waited_secs += 3


def get_or_create_registered_custom_model_version(
    endpoint: str,
    token: str,
    custom_model_version_id: str,
    registered_model_name: str,
    **kwargs: Any,
) -> str:
    """Get or create a registered model version from a custom model version with requested parameters.

    Notes
    -----
    Records a checksum in the registered model version description field to allow future calls to this
    function to validate whether a desired model version already exists.

    In addition to the standard arguments from the DataRobot SDK, this function accepts a `max_wait`
    argument to specify the maximum time to wait for the registered model version to build.
    """
    dr.Client(token=token, endpoint=endpoint)  # type: ignore[attr-defined]
    max_wait = kwargs.pop("max_wait", dr.enums.DEFAULT_MAX_WAIT)
    model_version_token = get_hash(custom_model_version_id, registered_model_name, **kwargs)

    try:
        existing_model = _find_existing_registered_model(registered_model_name)
        for model_version in existing_model.list_versions():
            description = model_version.model_description["description"]
            if description is not None and model_version_token in description:
                _await_registered_model_build(model_version, max_wait)
                return str(model_version.id)  # Return existing, matching version
        # Create incremental registered version
        kwargs["registered_model_id"] = existing_model.id
    except KeyError:
        # Create new registered model
        kwargs["registered_model_name"] = registered_model_name

    kwargs["description"] = kwargs.get("description", "") + f"\nChecksum: {model_version_token}"
    model_version = dr.RegisteredModelVersion.create_for_custom_model_version(  # type: ignore[attr-defined]
        custom_model_version_id,
        **kwargs,
    )
    _await_registered_model_build(registered_model_version=model_version, max_wait=max_wait)
    return str(model_version.id)


def get_or_create_registered_external_model_version(
    endpoint: str,
    token: str,
    name: str,
    target: ExternalTarget,
    registered_model_name: str,
    **kwargs: Any,
) -> str:
    """Get or create a registered model version for an external model with requested parameters.

    Notes
    -----
    Records a checksum in the registered model version description field to allow future calls to this
    function to validate whether a desired model version already exists.

    In addition to the standard arguments from the DataRobot SDK, this function accepts a `max_wait`
    argument to specify the maximum time to wait for the registered model version to build.
    """
    dr.Client(token=token, endpoint=endpoint)  # type: ignore[attr-defined]
    timeout_seconds = kwargs.pop("max_wait", 600)
    model_version_token = get_hash(name, target, registered_model_name, **kwargs)

    try:
        existing_model = _find_existing_registered_model(registered_model_name)
        for model_version in existing_model.list_versions():
            description = model_version.model_description["description"]
            if description is not None and model_version_token in description:
                _await_registered_model_build(model_version, timeout_seconds)
                return str(model_version.id)  # Return existing, matching version
        # Create incremental registered version
        kwargs["registered_model_id"] = existing_model.id
    except KeyError:
        # Create new registered model
        kwargs["registered_model_name"] = registered_model_name

    if "model_description" in kwargs:
        kwargs["model_description"]["description"] = (
            kwargs["model_description"].get("model_description", "")
            + f"\nChecksum: {model_version_token}"
        )
    else:
        kwargs["model_description"] = {"description": f"Checksum: {model_version_token}"}

    model_version = dr.RegisteredModelVersion.create_for_external(  # type: ignore[attr-defined]
        name,
        target,
        **kwargs,
    )
    _await_registered_model_build(model_version, timeout_seconds)
    return str(model_version.id)


def get_or_create_registered_leaderboard_model_version(
    endpoint: str,
    token: str,
    model_id: str,
    registered_model_name: str,
    **kwargs: Any,
) -> str:
    """Get or create a registered model version from a DataRobot leaderboard model with requested parameters.

    Notes
    -----
    Records a checksum in the registered model version description field to allow future calls to this
    function to validate whether a desired model version already exists.

    In addition to the standard arguments from the DataRobot SDK, this function accepts a `max_wait`
    argument to specify the maximum time to wait for the registered model version to build.
    """
    dr.Client(token=token, endpoint=endpoint)  # type: ignore[attr-defined]
    timeout_seconds = kwargs.pop("max_wait", dr.enums.DEFAULT_MAX_WAIT)
    model_version_token = get_hash(model_id, registered_model_name, **kwargs)

    try:
        existing_model = _find_existing_registered_model(registered_model_name)

        for model_version in existing_model.list_versions():
            description = model_version.model_description["description"]
            if description is not None and model_version_token in description:
                _await_registered_model_build(model_version, timeout_seconds)
                return str(model_version.id)  # Return existing, matching version
        # Create incremental registered version
        kwargs["registered_model_id"] = existing_model.id
    except KeyError:
        # Create new registered model
        kwargs["registered_model_name"] = registered_model_name

    kwargs["description"] = kwargs.get("description", "") + f"\nChecksum: {model_version_token}"
    model_version = dr.RegisteredModelVersion.create_for_leaderboard_item(  # type: ignore[attr-defined]
        model_id,
        **kwargs,
    )
    _await_registered_model_build(model_version, timeout_seconds)
    return str(model_version.id)
