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
import posixpath
from typing import Any, Optional

import requests

import datarobot as dr
from datarobot.rest import handle_http_error
from datarobot.utils import camelize


def _create_custom_model(
    endpoint: str,
    token: str,
    name: str,
    target_type: str,
    custom_model_type: str = "inference",
    target_name: Optional[str] = None,
    **kwargs: Any,
) -> str:
    url = posixpath.join(endpoint, "customModels/")
    body = {
        "name": name,
        "target_type": target_type,
        "custom_model_type": custom_model_type,
    }
    if target_name:
        body["custom_model_name"] = target_name
    body.update(kwargs)
    body = {camelize(k): v for k, v in body.items()}
    resp = requests.post(url, headers={"Authorization": f"Bearer {token}"}, json=body)
    if not resp:
        handle_http_error(resp)
    return str(resp.json()["id"])


def _find_existing_custom_model(**kwargs: Any) -> str:
    for model in dr.CustomInferenceModel.list():
        if all(getattr(model, key) == kwargs[key] for key in kwargs):
            return str(model.id)
    raise KeyError("No matching custom model found")


def get_or_create_custom_model(
    endpoint: str,
    token: str,
    name: str,
    target_type: str,
    target_name: Optional[str] = None,
    **kwargs: Any,
) -> str:
    """Get or create a custom model with requested parameters."""
    if target_type != "training" and target_name is None and target_type != "Anomaly":
        raise ValueError(
            "target_name is required for inference custom models that are not Anomaly Detection Models"
        )
    dr.Client(endpoint=endpoint, token=token)
    try:
        return _find_existing_custom_model(name=name, target_type=target_type, **kwargs)
    except KeyError:
        return _create_custom_model(
            endpoint=endpoint,
            token=token,
            name=name,
            target_type=target_type,
            target_name=target_name,
            **kwargs,
        )
