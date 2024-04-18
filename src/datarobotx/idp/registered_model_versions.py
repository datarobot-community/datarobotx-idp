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
from typing import Any

import datarobot as dr
from datarobot import RegisteredModel
from datarobot.models.model_registry.registered_model_version import (
    ExternalTarget,
)

from datarobotx.idp.common.hashing import get_hash


def _find_existing_registered_model(registered_model_name: str) -> RegisteredModel:
    for model in dr.RegisteredModel.list():
        if model.name == registered_model_name:
            return model
    raise KeyError("No matching registered model found")


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
    function to validate whether a desired model version already exists
    """
    dr.Client(token=token, endpoint=endpoint)
    model_version_token = get_hash(custom_model_version_id, registered_model_name, **kwargs)

    try:
        existing_model = _find_existing_registered_model(registered_model_name)
        for model_version in existing_model.list_versions():
            description = model_version.model_description["description"]
            if description is not None and model_version_token in description:
                return str(model_version.id)  # Return existing, matching version
        # Create incremental registered version
        kwargs["registered_model_id"] = existing_model.id
    except KeyError:
        # Create new registered model
        kwargs["registered_model_name"] = registered_model_name

    kwargs["description"] = kwargs.get("description", "") + f"\nChecksum: {model_version_token}"
    model_version = dr.RegisteredModelVersion.create_for_custom_model_version(
        custom_model_version_id,
        **kwargs,
    )
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
    function to validate whether a desired model version already exists
    """
    dr.Client(token=token, endpoint=endpoint)
    model_version_token = get_hash(name, target, registered_model_name, **kwargs)

    try:
        existing_model = _find_existing_registered_model(registered_model_name)
        for model_version in existing_model.list_versions():
            description = model_version.model_description["description"]
            if description is not None and model_version_token in description:
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

    model_version = dr.RegisteredModelVersion.create_for_external(
        name,
        target,
        **kwargs,
    )
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
    function to validate whether a desired model version already exists
    """
    dr.Client(token=token, endpoint=endpoint)
    model_version_token = get_hash(model_id, registered_model_name, **kwargs)

    try:
        existing_model = _find_existing_registered_model(registered_model_name)
        for model_version in existing_model.list_versions():
            description = model_version.model_description["description"]
            if description is not None and model_version_token in description:
                return str(model_version.id)  # Return existing, matching version
        # Create incremental registered version
        kwargs["registered_model_id"] = existing_model.id
    except KeyError:
        # Create new registered model
        kwargs["registered_model_name"] = registered_model_name

    kwargs["description"] = kwargs.get("description", "") + f"\nChecksum: {model_version_token}"
    model_version = dr.RegisteredModelVersion.create_for_leaderboard_item(
        model_id,
        **kwargs,
    )
    return str(model_version.id)
