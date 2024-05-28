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

import datarobot as dr
from datarobot.rest import handle_http_error
from datarobot.utils import camelize
from datarobot.utils.pagination import unpaginate
from datarobot.utils.waiters import wait_for_async_resolution


def _create_custom_app(
    endpoint: str,
    token: str,
    name: str,
    environment_id: Optional[str],
    custom_application_source_version_id: Optional[str],
    **kwargs: Any,
) -> str:
    client = dr.Client(endpoint=endpoint, token=token)
    body = {"name": name}
    if environment_id is not None:
        body["environmentId"] = environment_id
    if custom_application_source_version_id is not None:
        body["application_source_version_id"] = custom_application_source_version_id
    body.update(kwargs)
    body = {camelize(k): v for k, v in body.items()}

    initial_resp = client.post("customApplications/", json=body)
    if not initial_resp:
        handle_http_error(initial_resp)

    status_location = initial_resp.headers["location"]
    app_url = wait_for_async_resolution(
        dr.Client(token=token, endpoint=endpoint),
        status_location,
        max_wait=30 * 60,
    )
    return str(app_url).split("/")[-2]


def _delete_custom_app(endpoint: str, token: str, custom_app_id: str) -> None:
    url = posixpath.join(endpoint, f"customApplications/{custom_app_id}/")
    dr.Client(endpoint=endpoint, token=token).delete(url)


def _list_custom_apps(endpoint: str, token: str, name: Optional[str] = None) -> Any:
    url = posixpath.join(endpoint, "customApplications/")
    client = dr.Client(endpoint=endpoint, token=token)
    params = {"name": name} if name is not None else None
    try:
        for app in unpaginate(initial_url=url, initial_params=params, client=client):
            yield app
    except KeyError:
        return  # custom apps route has a pagination bug


def _find_existing_custom_app(endpoint: str, token: str, **kwargs: Any) -> str:
    for app in _list_custom_apps(endpoint, token):
        if all([app[camelize(key)] == kwargs[key] for key in kwargs if kwargs[key] is not None]):
            return str(app["id"])
    raise KeyError("No matching custom app found")


def get_replace_or_create_custom_app(
    endpoint: str,
    token: str,
    name: str,
    environment_id: Optional[str] = None,
    env_version_id: Optional[str] = None,
    custom_application_source_version_id: Optional[str] = None,
    **kwargs: Any,
) -> str:
    """Get, replace, or create a custom application from a custom environment with requested parameters.

    If a custom app with the desired name already exists but with different parameters, the existing
    app will be deleted and a new one created.

    Exactly one of env_version_id or custom_application_source_version_id is required.

    Args:
        endpoint: The DataRobot endpoint.
        token: The DataRobot token.
        name: The name of the custom application.
        environment_id: The environment ID.
        env_version_id: The environment version ID.
        custom_application_source_version_id: The custom application source version ID.
        **kwargs: Additional keyword arguments to pass to the custom application creation endpoint.

    Returns
    -------
        The ID of the custom application.
    """
    # test if env_version_id xor custom_application_source_version_id is provided
    if bool(env_version_id) == bool(custom_application_source_version_id):
        raise ValueError(
            "Exactly one of env_version_id or custom_application_source_version_id is required"
        )

    try:
        custom_app_id = _find_existing_custom_app(endpoint, token, name=name)
        try:
            return _find_existing_custom_app(
                endpoint,
                token,
                name=name,
                env_version_id=env_version_id,
                custom_application_source_version_id=custom_application_source_version_id,
                **kwargs,
            )
        except KeyError:
            _delete_custom_app(endpoint, token, custom_app_id)
    except KeyError:
        pass

    return _create_custom_app(
        endpoint=endpoint,
        token=token,
        name=name,
        environment_id=environment_id,
        custom_application_source_version_id=custom_application_source_version_id,
        **kwargs,
    )


def get_replace_or_create_custom_app_from_env(
    endpoint: str,
    token: str,
    name: str,
    environment_id: str,
    env_version_id: str,
    **kwargs: Any,
) -> str:
    """Get, replace, or create a custom application from a custom environment with requested parameters.

    If a custom app with the desired name already exists but with different parameters, the existing
    app will be deleted and a new one created.

    Args:
        endpoint: The DataRobot endpoint.
        token: The DataRobot token.
        name: The name of the custom application.
        environment_id: The environment ID.
        env_version_id: The environment version ID.
        **kwargs: Additional keyword arguments to pass to the custom application creation endpoint.

    Returns
    -------
        The ID of the custom application.
    """
    return get_replace_or_create_custom_app(
        endpoint=endpoint,
        token=token,
        name=name,
        environment_id=environment_id,
        env_version_id=env_version_id,
        **kwargs,
    )
