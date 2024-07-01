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

import contextlib
import json
import os
from pathlib import Path
import posixpath
from typing import Any, Dict, List, Optional, Tuple

import requests
from requests_toolbelt import MultipartEncoder

import datarobot as dr
from datarobot.rest import handle_http_error
from datarobot.utils import camelize, to_api
from datarobot.utils.pagination import unpaginate

from datarobotx.idp.common.hashing import get_hash


def _create_or_update_custom_job(
    endpoint: str,
    token: str,
    folder_path: str,
    entry_point: str,
    custom_job_id: Optional[str] = None,
    runtime_parameter_values: Optional[List[Dict[str, Any]]] = None,
    schedule: Optional[Dict[str, Any]] = None,
    **kwargs: Any,
) -> str:
    url = posixpath.join(endpoint, "customJobs/")
    headers = {"Authorization": f"Bearer {token}"}

    resp = None
    with contextlib.ExitStack() as stack:
        primary_form_data: List[Tuple[str, Any]] = [
            (camelize(k), json.dumps(to_api(v)) if not isinstance(v, str) else v)
            for k, v in kwargs.items()
        ]
        for root_path, _, file_paths in os.walk(folder_path):
            for path in file_paths:
                file_path = os.path.join(root_path, path)
                file = stack.enter_context(open(file_path, "rb"))
                primary_form_data.append(("file", (os.path.basename(file_path), file)))
                primary_form_data.append(("filePath", os.path.relpath(file_path, folder_path)))
        encoder = MultipartEncoder(fields=primary_form_data)
        headers["Content-Type"] = encoder.content_type
        if custom_job_id is None:
            resp = requests.post(url, headers=headers, data=encoder)
        else:
            _clear_existing_files(endpoint, token, custom_job_id)
            resp = requests.patch(url + f"{custom_job_id}/", headers=headers, data=encoder)
        if not resp:
            handle_http_error(resp)
        custom_job_id = str(resp.json()["id"])

        secondary_form_data = {
            "entryPoint": _get_entry_point_id(endpoint, token, custom_job_id, entry_point)
        }
        if runtime_parameter_values is not None:
            secondary_form_data["runtimeParameterValues"] = json.dumps(
                [to_api(param) for param in runtime_parameter_values]
            )
        if schedule is not None:
            secondary_form_data["schedule"] = json.dumps({"schedule": to_api(schedule)})
        headers["Content-Type"] = "application/json"
        resp = requests.patch(url + f"{custom_job_id}/", headers=headers, json=secondary_form_data)
        if not resp:
            handle_http_error(resp)

    assert resp is not None
    return str(resp.json()["id"])


def _clear_existing_files(endpoint: str, token: str, custom_job_id: str) -> None:
    url = posixpath.join(endpoint, f"customJobs/{custom_job_id}/")
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url=url, headers=headers)
    if not resp:
        handle_http_error(resp)
    to_delete = [file["id"] for file in resp.json()["items"]]
    resp = requests.patch(url=url, headers=headers, json={"filesToDelete": to_delete})
    if not resp:
        handle_http_error(resp)


def _get_entry_point_id(endpoint: str, token: str, custom_job_id: str, entry_point: str) -> str:
    url = posixpath.join(endpoint, f"customJobs/{custom_job_id}/")
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url=url, headers=headers)
    if not resp:
        handle_http_error(resp)

    for file in resp.json()["items"]:
        if file["filePath"] == entry_point:
            return str(file["id"])
    else:
        msg = f"The requested entrypoint '{entry_point}' does not exist in custom job '{custom_job_id}'"
        raise ValueError(msg)


def _find_existing_custom_job(
    endpoint: str, token: str, name: str, custom_job_token: str
) -> Tuple[str, str]:
    url = posixpath.join(endpoint, "customJobs/")
    client = dr.Client(endpoint=endpoint, token=token)  # type: ignore[attr-defined]
    for job in unpaginate(initial_url=url, initial_params=None, client=client):
        if job["name"] == name:
            if custom_job_token in job["description"]:
                return str(job["id"]), "get"
            else:
                return str(job["id"]), "patch"
    raise KeyError("No matching custom job found")


def get_replace_or_create_custom_job(
    endpoint: str,
    token: str,
    name: str,
    folder_path: str,
    entry_point: str,
    **kwargs: Any,
) -> str:
    """Get, replace, or create a custom job with requested parameters, files.

    If a custom job with the requested name and parameters already exists, return its ID.
    If a custom job with the requested name exists with different parameters, it will be updated in place.
    If no custom job with the requested name exists, it will be created.

    Parameters
    ----------
    name : str
        The name of the custom job
    folder_path : str
        Path containing files to be uploaded into the custom job
    entry_point : str
        Relative path to the file that is the entry point for the custom job.

    Notes
    -----
    Records a checksum in the custom job description field to allow future calls to this
    function to validate whether a desired custom job already exists with the same
    parameters, files.
    """
    custom_job_token = get_hash(
        name,
        entry_point,
        Path(folder_path),
        **kwargs,
    )
    kwargs["description"] = kwargs.get("description", "") + f"\nChecksum: {custom_job_token}"
    try:
        job_id, status = _find_existing_custom_job(endpoint, token, name, custom_job_token)
        if status == "patch":
            _create_or_update_custom_job(
                endpoint,
                token,
                folder_path,
                entry_point,
                custom_job_id=job_id,
                name=name,
                **kwargs,
            )
        return job_id
    except KeyError:
        return _create_or_update_custom_job(
            endpoint, token, folder_path, entry_point, name=name, **kwargs
        )
