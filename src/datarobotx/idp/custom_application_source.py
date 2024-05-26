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

import contextlib
import json
import os
from pathlib import Path
import time
from typing import Any, Dict, List, Optional, Tuple, Union

from requests_toolbelt import MultipartEncoder

import datarobot as dr
from datarobot.models.runtime_parameters import RuntimeParameterValue
from datarobot.utils import camelize

from datarobotx.idp.common.hashing import get_hash


def get_or_create_custom_application_source(endpoint: str, token: str, name: str) -> str:
    """Get or create a custom application source with the given name.

    Args:
        endpoint: The DataRobot endpoint.
        token: The DataRobot API token.
        name: The name of the custom application source.

    Returns
    -------
            The ID of the custom application source.
    """
    if endpoint is not None and token is not None:
        client = dr.Client(endpoint=endpoint, token=token)  # type: ignore
    else:
        client = dr.client.get_client()

    custom_application_source = client.get("customApplicationSources/").json()["data"]

    for custom_application_source in custom_application_source:
        if custom_application_source["name"] == name:
            return str(custom_application_source["id"])

    create_custom_application_source_response = client.post(
        "customApplicationSources/",
    )
    custom_application_source_id = create_custom_application_source_response.json()["id"]

    client.patch(
        f"customApplicationSources/{custom_application_source_id}",
        json={"name": name},
    )
    return str(custom_application_source_id)


# Helper functions
def _find_existing_custom_application_source_version(
    endpoint: str, token: str, custom_application_source_id: str, version_token: str
) -> str:
    client = dr.Client(endpoint=endpoint, token=token)  # type: ignore
    versions = client.get(
        f"customApplicationSources/{custom_application_source_id}/versions/"
    ).json()["data"]
    for version in versions:
        if version.get("label") and version_token in version["label"]:
            return str(version["id"])
    raise KeyError("No matching application source version found")


def _get_or_create_custom_application_source_version(
    from_previous: bool,
    endpoint: str,
    token: str,
    custom_application_source_id: str,
    previous_custom_application_source_version_id: Optional[str] = None,
    runtime_parameter_values: Optional[List[Union[RuntimeParameterValue, Dict[str, str]]]] = None,
    **kwargs: Any,
) -> str:
    client = dr.Client(token=token, endpoint=endpoint)  # type: ignore

    if runtime_parameter_values is not None:
        runtime_parameter_values_objs = [
            RuntimeParameterValue(**param) if isinstance(param, dict) else param
            for param in runtime_parameter_values
        ]
    else:
        runtime_parameter_values_objs = []

    folder_path = kwargs.pop("folder_path", None)

    if from_previous and not previous_custom_application_source_version_id:
        raise ValueError("Must provide a previous version ID when creating from previous version.")

    if from_previous:
        # Get the previous version's details and use them as the base for the new version
        previous_version = client.get(
            f"customApplicationSources/{custom_application_source_id}/versions/{previous_custom_application_source_version_id}/"
        ).json()
        version_token = previous_version["label"].split("\nChecksum: ")[-1]
    else:
        version_token = get_hash(
            Path(folder_path) if folder_path else None,
            custom_application_source_id,
            runtime_parameter_values=runtime_parameter_values_objs,
            **kwargs,
        )

    try:
        existing_version_id = _find_existing_custom_application_source_version(
            endpoint=endpoint,
            token=token,
            custom_application_source_id=custom_application_source_id,
            version_token=version_token,
        )
        return existing_version_id
    except KeyError:
        upload_data: List[Tuple[str, Any]] = []
        with contextlib.ExitStack() as stack:
            if folder_path:
                for root_path, _, file_paths in os.walk(folder_path):
                    for path in file_paths:
                        file_path = os.path.join(root_path, path)
                        file = stack.enter_context(open(file_path, "rb"))
                        upload_data.append(("file", (os.path.basename(file_path), file)))
                        upload_data.append(("filePath", os.path.relpath(file_path, folder_path)))

            if runtime_parameter_values_objs:
                upload_data.append(
                    (
                        "runtimeParameterValues",
                        json.dumps(
                            [
                                {camelize(k): v for k, v in param.to_dict().items()}
                                for param in runtime_parameter_values_objs
                            ]
                        ),
                    )
                )

            if "replicas" in kwargs:
                upload_data.append(("replicas", str(kwargs["replicas"])))

            if base_environment_id := kwargs.get("base_environment_id"):
                upload_data.append(("baseEnvironmentId", base_environment_id))

            if base_environment_version_id := kwargs.get("base_environment_version_id"):
                upload_data.append(("baseEnvironmentVersionId", base_environment_version_id))
            upload_data.append(
                (
                    "label",
                    f"{kwargs.get('label', '')}{' - ' if kwargs.get('label') else ''}[{version_token}]",
                )
            )

            encoder = MultipartEncoder(fields=upload_data)
            headers = {"Content-Type": encoder.content_type}

            if from_previous:
                response = client.request(
                    method="PATCH",
                    url=f"customApplicationSources/{custom_application_source_id}/versions/{previous_custom_application_source_version_id}/",
                    data=encoder,
                    headers=headers,
                )
            else:
                response = client.request(
                    method="POST",
                    url=f"customApplicationSources/{custom_application_source_id}/versions/",
                    data=encoder,
                    headers=headers,
                )
            new_version = response.json()["id"]

        return str(new_version)


# Public functions
def get_or_create_custom_application_source_version(
    endpoint: str,
    token: str,
    custom_application_source_id: str,
    runtime_parameter_values: Optional[List[Union[RuntimeParameterValue, Dict[str, str]]]] = None,
    **kwargs: Any,
) -> str:
    """Get or create a custom application source version.

    Parameters
    ----------
    endpoint : str
        The DataRobot endpoint.
    token : str
        The DataRobot API token.
    custom_application_source_id : str
        runtime_parameter_values : Optional[List[Union[RuntimeParameterValue, Dict[str, str]]]]
    kwargs : Any

    Returns
    -------
    str
        The ID of the custom application source version.
    """
    return _get_or_create_custom_application_source_version(
        from_previous=False,
        endpoint=endpoint,
        token=token,
        custom_application_source_id=custom_application_source_id,
        runtime_parameter_values=runtime_parameter_values,
        **kwargs,
    )


def get_or_create_custom_application_source_version_from_previous(
    endpoint: str,
    token: str,
    custom_application_source_id: str,
    previous_custom_application_source_version_id: str,
    runtime_parameter_values: Optional[List[Union[RuntimeParameterValue, Dict[str, str]]]] = None,
    **kwargs: Any,
) -> str:
    """Get or create a custom application source version from a previous version.

    Parameters
    ----------
    endpoint : str
        The DataRobot endpoint.
    token : str
        The DataRobot API token.
    custom_application_source_id : str
    previous_custom_application_source_version_id : str
    runtime_parameter_values : Optional[List[Union[RuntimeParameterValue, Dict[str, str]]]]
    kwargs : Any

    Returns
    -------
    str
        The ID of the custom application source version.
    """
    return _get_or_create_custom_application_source_version(
        from_previous=True,
        endpoint=endpoint,
        token=token,
        custom_application_source_id=custom_application_source_id,
        previous_custom_application_source_version_id=previous_custom_application_source_version_id,
        runtime_parameter_values=runtime_parameter_values,
        **kwargs,
    )


def get_or_create_qanda_app(
    endpoint: Optional[str],
    token: Optional[str],
    deployment_id: str,
    environment_id: str,
    name: str,
) -> str:
    """Get or create a Q&A app.

    Parameters
    ----------
    endpoint : Optional[str]
        The DataRobot endpoint.
    token : Optional[str]
        The DataRobot API token.
    deployment_id : str
        The ID of the deployment.
    environment_id : str
        The ID of the environment.
    name : str
        The name of the Q&A app.

    Returns
    -------
    str
        The ID of the Q&A app.
    """
    app_token = get_hash(name, deployment_id, environment_id)

    if endpoint is not None and token is not None:
        client = dr.Client(endpoint=endpoint, token=token)  # type: ignore
    else:
        client = dr.client.get_client()

    try:
        apps = client.get("customApplications/").json()["data"]
        for app in apps:
            if app_token in app["name"]:
                return str(app["id"])
    except:
        pass

    quanda_app_response = client.post(
        "customApplications/qanda/",
        json={"deploymentId": deployment_id, "environmentId": environment_id},
    ).json()

    client.patch(
        f"customApplications/{quanda_app_response['id']}",
        json={
            "name": f"{name} [{app_token}]",
        },
    )
    client.patch(
        f"customApplicationSources/{quanda_app_response['customApplicationSourceId']}/",
        json={
            "name": f"{name} [{app_token}]",
        },
    )

    return str(quanda_app_response["id"])


def wait_for_app_ready(
    endpoint: str, token: str, custom_application_id: str, max_wait: int
) -> None:
    """Wait for the custom application to be ready.

    Parameters
    ----------
    client : dr.Client
        DataRobot client
    custom_application_id : str
        DataRobot id of the custom application
    max_wait : int
        Maximum time to wait in seconds
    """
    client = dr.Client(endpoint=endpoint, token=token)  # type: ignore

    start_time = time.time()
    while time.time() - start_time < max_wait:
        app = client.get(f"customApplications/{custom_application_id}")
        if app.json()["status"] == "running":
            break
        time.sleep(5)
    else:
        raise TimeoutError("Application did not become ready in time")
