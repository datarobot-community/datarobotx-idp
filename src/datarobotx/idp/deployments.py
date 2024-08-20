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

import posixpath
from typing import Any, Tuple

import requests

import datarobot as dr
from datarobot.rest import handle_http_error

from datarobotx.idp.common.hashing import get_hash


def _find_existing_deployment(deployment_token: str) -> str:
    for deployment in dr.Deployment.list(search=deployment_token):  # type: ignore[attr-defined]
        if (
            deployment.description is not None
            and deployment_token in deployment.description
            and deployment.status == "active"
        ):
            return str(deployment.id)
    raise KeyError("No matching deployment found")


def _lookup_registered_model_version(
    endpoint: str, token: str, deployment_id: str
) -> Tuple[str, str]:
    url = posixpath.join(endpoint, f"deployments/{deployment_id}")
    resp = requests.get(url, headers={"Authorization": f"Bearer {token}"})
    if not resp:
        handle_http_error(resp)
    json_ = resp.json()
    return (
        str(json_["modelPackage"]["id"]),
        str(json_["modelPackage"]["registeredModelId"]),
    )


def get_or_create_deployment_from_registered_model_version(
    endpoint: str,
    token: str,
    registered_model_version_id: str,
    label: str,
    **kwargs: Any,
) -> str:
    """Get or create a deployment with requested parameters from a registered model version.

    Notes
    -----
    Records a checksum in the deployment description field to allow future calls to this
    function to validate whether a desired deployment already exists with the same
    parameters and registered model version.
    """
    dr.Client(token=token, endpoint=endpoint)  # type: ignore[attr-defined]
    max_wait = kwargs.pop("max_wait", dr.enums.DEFAULT_MAX_WAIT)
    deployment_token = get_hash(registered_model_version_id, label, **kwargs)

    try:
        return _find_existing_deployment(deployment_token)
    except KeyError:
        kwargs["description"] = kwargs.get("description", "") + f"\nChecksum: {deployment_token}"
        deployment = dr.Deployment.create_from_registered_model_version(  # type: ignore[attr-defined]
            registered_model_version_id,
            label,
            max_wait=max_wait,
            **kwargs,
        )
        return str(deployment.id)


def get_replace_or_create_deployment_from_registered_model(
    endpoint: str,
    token: str,
    registered_model_version_id: str,
    registered_model_name: str,
    label: str,
    reason: str = "OTHER",
    **kwargs: Any,
) -> str:
    """Get, replace, or create a deployment with requested parameters from a registered model.

    Notes
    -----
    Records a checksum in the deployment description field to allow future calls to this
    function to validate whether a desired deployment already exists with the same
    parameters and associated registered model name (unique).

    If checksum matches, deployment will be replaced in place if the requested registered
    model version is different from what is already deployed.
    """
    dr.Client(token=token, endpoint=endpoint)  # type: ignore[attr-defined]
    max_wait = kwargs.pop("max_wait", dr.enums.DEFAULT_MAX_WAIT)
    deployment_token = get_hash(registered_model_name, label, **kwargs)

    try:
        curr_deployment_id = _find_existing_deployment(deployment_token)
        (
            curr_registered_model_version_id,
            curr_registered_model_id,
        ) = _lookup_registered_model_version(endpoint, token, curr_deployment_id)
        if registered_model_version_id != curr_registered_model_version_id:
            dr.Deployment.get(curr_deployment_id).perform_model_replace(  # type: ignore[attr-defined]
                new_registered_model_version_id=registered_model_version_id,
                reason=reason,
                max_wait=max_wait,
            )
        return curr_deployment_id
    except KeyError:
        kwargs["description"] = kwargs.get("description", "") + f"\nChecksum: {deployment_token}"
        deployment = dr.Deployment.create_from_registered_model_version(  # type: ignore[attr-defined]
            registered_model_version_id,
            label,
            max_wait=max_wait,
            **kwargs,
        )
        return str(deployment.id)
