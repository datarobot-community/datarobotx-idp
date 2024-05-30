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
from typing import Any

import requests

import datarobot as dr
from datarobot.rest import handle_http_error
from datarobot.utils import camelize
from datarobot.utils.pagination import unpaginate


def _create_execution_environment(
    endpoint: str,
    token: str,
    name: str,
    **kwargs: Any,
) -> str:
    url = posixpath.join(endpoint, "executionEnvironments/")
    body = {"name": name}
    body.update(kwargs)
    body = {camelize(k): v for k, v in body.items()}
    resp = requests.post(url, headers={"Authorization": f"Bearer {token}"}, json=body)
    if not resp:
        handle_http_error(resp)
    return str(resp.json()["id"])


def _list_execution_environments(endpoint: str, token: str) -> Any:
    url = posixpath.join(endpoint, "executionEnvironments/")
    client = dr.Client(endpoint=endpoint, token=token)  # type: ignore[attr-defined]
    for env in unpaginate(initial_url=url, initial_params=None, client=client):
        yield env


def _find_existing_environment(endpoint: str, token: str, **kwargs: Any) -> str:
    for env in _list_execution_environments(endpoint, token):
        if all([env[camelize(key)] == kwargs[key] for key in kwargs]):
            return str(env["id"])
    raise KeyError("No matching environment found")


def get_or_create_execution_environment(endpoint: str, token: str, name: str, **kwargs: Any) -> str:
    """Get or create an execution environment with requested parameters."""
    try:
        return _find_existing_environment(endpoint, token, name=name, **kwargs)
    except KeyError:
        return _create_execution_environment(endpoint, token, name, **kwargs)
