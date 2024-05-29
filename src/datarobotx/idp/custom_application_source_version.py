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
from typing import Any, Dict, List, Optional, Tuple, Union

from requests_toolbelt import MultipartEncoder

import datarobot as dr
from datarobot.models.runtime_parameters import RuntimeParameterValue
from datarobot.utils import camelize

from datarobotx.idp.common.hashing import get_hash


def _find_existing_custom_application_source_version(
    endpoint: str, token: str, custom_application_source_id: str, version_token: str
) -> str:
    client = dr.Client(endpoint=endpoint, token=token)  # type: ignore
    versions = client.get(
        f"customApplicationSources/{custom_application_source_id}/versions/"
    ).json()["data"]
    for version in versions:
        if version_token in version.get("label", ""):
            return str(version["id"])
    raise KeyError("No matching application source version found")


def _get_or_create_custom_application_source_version(
    endpoint: str,
    token: str,
    custom_application_source_id: str,
    runtime_parameter_values: Optional[List[Union[RuntimeParameterValue, Dict[str, str]]]] = None,
    previous_custom_application_source_version_id: Optional[str] = None,
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

            if previous_custom_application_source_version_id is not None:
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
    **kwargs: Any,
) -> str:
    """Get or create a custom application source version.

    Parameters
    ----------
    custom_application_source_id : str
        The ID of the custom application source.
    **kwargs : Any
        Additional keyword arguments.

    Returns
    -------
    str
        The ID of the custom application source version.
    """
    runtime_parameter_values = kwargs.pop("runtime_parameter_values", None)

    app_source_version_id = _get_or_create_custom_application_source_version(
        endpoint=endpoint,
        token=token,
        custom_application_source_id=custom_application_source_id,
        runtime_parameter_values=runtime_parameter_values,
        **kwargs,
    )
    if runtime_parameter_values is not None:
        app_source_version_id = _get_or_create_custom_application_source_version(
            endpoint=endpoint,
            token=token,
            custom_application_source_id=custom_application_source_id,
            previous_custom_application_source_version_id=app_source_version_id,
            runtime_parameter_values=runtime_parameter_values,
            **kwargs,
        )
    return app_source_version_id


def get_or_create_custom_application_source_version_from_previous(
    endpoint: str,
    token: str,
    custom_application_source_id: str,
    runtime_parameter_values: Optional[List[Union[RuntimeParameterValue, Dict[str, str]]]] = None,
    **kwargs: Any,
) -> str:
    """Get or create a custom application source version from a previous version.

    Parameters
    ----------
    custom_application_source_id : str
        The ID of the custom application source.
    runtime_parameter_values : Optional[List[Union[RuntimeParameterValue, Dict[str, str]]]], optional
        A list of runtime parameter values. Each value can be either a `RuntimeParameterValue` object or a dictionary
        with parameter names as keys and parameter values as values. (default: None)
    **kwargs : Any
        Additional keyword arguments.

    Returns
    -------
    str
        The ID of the custom application source version.
    """
    if runtime_parameter_values is not None:
        runtime_parameter_values_objs = [
            RuntimeParameterValue(**d) for d in runtime_parameter_values if isinstance(d, dict)
        ]
    else:
        runtime_parameter_values_objs = None

    client = dr.Client(token=token, endpoint=endpoint)  # type: ignore
    app_version_token = get_hash(
        custom_application_source_id,
        runtime_parameter_values=runtime_parameter_values_objs,
        **kwargs,
    )
    try:
        previous_custom_application_source_version_id = (
            _find_existing_custom_application_source_version(
                endpoint=endpoint,
                token=token,
                custom_application_source_id=custom_application_source_id,
                version_token=app_version_token,
            )
        )
        return previous_custom_application_source_version_id
    except KeyError:
        try:
            all_versions = client.get(
                f"customApplicationSources/{custom_application_source_id}/versions/"
            ).json()["data"]
            all_versions.sort(key=lambda x: x["createdAt"], reverse=True)
            previous_custom_application_source_version_id = all_versions[0]["id"]
            new_version = client.post(
                f"customApplicationSources/{custom_application_source_id}/versions/",
                json={
                    "baseVersion": previous_custom_application_source_version_id,
                    "label": f"{kwargs.get('label', '')}{' - ' if kwargs.get('label') else ''}[{app_version_token}]",
                },
            ).json()["id"]
            return _get_or_create_custom_application_source_version(
                endpoint=endpoint,
                token=token,
                custom_application_source_id=custom_application_source_id,
                previous_custom_application_source_version_id=new_version,
                runtime_parameter_values=runtime_parameter_values,
                **kwargs,
            )
        except:
            raise ValueError("No previous version found")
