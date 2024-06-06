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
from json import dumps
import os
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Tuple

from requests_toolbelt import MultipartEncoder

import datarobot as dr
from datarobot.models.runtime_parameters import RuntimeParameterValue
from datarobot.utils import camelize

from datarobotx.idp.common.hashing import get_hash


def _find_existing_custom_application_source_version(
    client: dr.Client,  # type: ignore
    custom_application_source_id: str,
    version_token: str,
    from_previous: bool,
) -> str:
    versions = client.get(
        f"customApplicationSources/{custom_application_source_id}/versions/"
    ).json()["data"]

    if from_previous:
        versions.sort(key=lambda x: x["createdAt"], reverse=True)
        versions = [versions[0]] if len(versions) else []
    for version in versions:
        if version["label"] is not None and version_token in version["label"]:
            return str(version["id"])
    raise KeyError("No matching application source version found")


def _make_request(
    client: dr.Client,  # type: ignore
    method: Literal["POST", "PATCH", "GET"],
    partial_url: str,
    json: Optional[Dict[str, Any]] = None,
    **kwargs: Any,
) -> Any:
    if method != "GET" and json is None:
        upload_data: List[Tuple[str, Any]] = []
        with contextlib.ExitStack() as stack:
            if "folder_path" in kwargs:
                folder_path = kwargs["folder_path"]
                for root_path, _, file_paths in os.walk(folder_path):
                    for path in file_paths:
                        file_path = os.path.join(root_path, path)
                        file = stack.enter_context(open(file_path, "rb"))
                        upload_data.append(("file", (os.path.basename(file_path), file)))
                        upload_data.append(("filePath", os.path.relpath(file_path, folder_path)))

            if "runtime_parameter_values" in kwargs:
                upload_data.append(
                    (
                        "runtimeParameterValues",
                        dumps(
                            [
                                {camelize(k): v for k, v in param.to_dict().items()}
                                for param in kwargs["runtime_parameter_values"]
                            ]
                        ),
                    )
                )

            if "replicas" in kwargs:
                upload_data.append(("replicas", str(kwargs["replicas"])))

            if "base_environment_id" in kwargs:
                upload_data.append(("baseEnvironmentId", kwargs["base_environment_id"]))

            if "base_environment_version_id" in kwargs:
                upload_data.append(
                    ("baseEnvironmentVersionId", kwargs["base_environment_version_id"])
                )
            if "label" in kwargs:
                upload_data.append(("label", kwargs["label"]))

            encoder = MultipartEncoder(fields=upload_data)
            headers = {"Content-Type": encoder.content_type}
            response = client.request(
                method=method,
                url=partial_url,
                data=encoder,
                headers=headers,
            )
    else:
        response = client.request(
            method=method,
            url=partial_url,
            json=json,
        )
    return response.json()


def _get_or_create_custom_application_source_version(
    from_previous: bool,
    endpoint: str,
    token: str,
    custom_application_source_id: str,
    **kwargs: Any,
) -> str:
    client = dr.Client(token=token, endpoint=endpoint)  # type: ignore

    version_token = get_hash(
        Path(kwargs["folder_path"]) if "folder_path" in kwargs else None,
        custom_application_source_id,
        **kwargs,
    )

    runtime_parameter_values_objs = []
    if "runtime_parameter_values" in kwargs:
        runtime_parameter_values_objs = [
            RuntimeParameterValue(**param) if isinstance(param, dict) else param
            for param in kwargs.pop("runtime_parameter_values")
        ]

    label = kwargs.pop("label", "")
    if len(label):
        label += " - "
    label += f"[{version_token}]"

    try:
        existing_version_id = _find_existing_custom_application_source_version(
            client=client,
            custom_application_source_id=custom_application_source_id,
            version_token=version_token,
            from_previous=from_previous,
        )
        return existing_version_id
    except KeyError:
        pass

    if from_previous:
        all_versions = _make_request(
            client=client,
            method="GET",
            partial_url=f"customApplicationSources/{custom_application_source_id}/versions/",
        )["data"]
        all_versions.sort(key=lambda x: x["createdAt"], reverse=True)
        if len(all_versions) == 0:
            raise ValueError(
                "Creating from a previous version is not possible: no previous version exists"
            )
        previous_custom_application_source_version_id = all_versions[0]["id"]
        new_version_id = str(
            _make_request(
                client=client,
                method="POST",
                partial_url=f"customApplicationSources/{custom_application_source_id}/versions/",
                json={
                    "baseVersion": previous_custom_application_source_version_id,
                    "label": label,
                },
            )["id"]
        )
        _make_request(
            client=client,
            method="PATCH",
            partial_url=(
                f"customApplicationSources/{custom_application_source_id}/"
                f"versions/{new_version_id}/"
            ),
            **kwargs,
        )
    else:
        new_version_id = str(
            _make_request(
                client=client,
                method="POST",
                partial_url=f"customApplicationSources/{custom_application_source_id}/versions/",
                label=label,
                **kwargs,
            )["id"]
        )

    # runtime parameter values must be patched subsequent to any possible file uploads/patching
    if len(runtime_parameter_values_objs):
        _make_request(
            client=client,
            method="PATCH",
            partial_url=(
                f"customApplicationSources/{custom_application_source_id}/"
                f"versions/{new_version_id}/"
            ),
            runtime_parameter_values=runtime_parameter_values_objs,
        )
    return new_version_id


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
    return _get_or_create_custom_application_source_version(
        from_previous=False,
        endpoint=endpoint,
        token=token,
        custom_application_source_id=custom_application_source_id,
        **kwargs,
    )


def _unsafe_get_or_create_custom_application_source_version_from_previous(
    endpoint: str,
    token: str,
    custom_application_source_id: str,
    **kwargs: Any,
) -> str:
    """Get or create a custom application source version from a previous version.

    Unsafe here means that idempotency is not guaranteed! Running this function multiple times with different arguments can lead to undefined behaviour.

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
    return _get_or_create_custom_application_source_version(
        from_previous=True,
        endpoint=endpoint,
        token=token,
        custom_application_source_id=custom_application_source_id,
        **kwargs,
    )
