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
# mypy: disable-error-code="attr-defined"
# pyright: reportPrivateImportUsage=false
import posixpath
from typing import Any

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
    **kwargs: Any,
) -> str:
    url = posixpath.join(endpoint, "customModels/")
    body = {
        "name": name,
        "target_type": target_type,
        "custom_model_type": custom_model_type,
    }
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
    endpoint: str, token: str, name: str, target_type: str, **kwargs: Any
) -> str:
    """Get or create a custom model with requested parameters."""
    dr.Client(endpoint=endpoint, token=token)
    try:
        return _find_existing_custom_model(name=name, target_type=target_type, **kwargs)
    except KeyError:
        return _create_custom_model(endpoint, token, name, target_type, **kwargs)
