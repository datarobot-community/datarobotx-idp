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
from typing import Any, Literal

import requests

import datarobot as dr
from datarobot.rest import handle_http_error
from datarobot.utils import camelize

from datarobotx.idp.common.hashing import get_hash


def _create_credential(
    endpoint: str, token: str, name: str, credential_type: str, **kwargs: Any
) -> str:
    url = posixpath.join(endpoint, "credentials/")
    body = {"name": name, "credential_type": credential_type}
    body.update(kwargs)
    body = {camelize(k): v for k, v in body.items()}
    resp = requests.post(url, headers={"Authorization": f"Bearer {token}"}, json=body, timeout=60)
    if not resp:
        handle_http_error(resp)
    return str(resp.json()["credentialId"])


def _get_or_update_or_delete_existing_credential(
    name: str, credential_type: str, credential_token: str, **kwargs: Any
) -> str:
    for credential in dr.Credential.list():  # type: ignore[attr-defined]
        if credential.name == name:
            if credential.description is not None and credential_token in credential.description:
                return str(credential.credential_id)
            elif credential.credential_type == credential_type:
                credential.update(description=f"Checksum: {credential_token}", **kwargs)
                return str(credential.credential_id)
            else:
                credential.delete()
                break
    raise KeyError("No matching credential found")


def get_replace_or_create_credential(
    endpoint: str,
    token: str,
    name: str,
    credential_type: Literal[
        "basic",
        "s3",
        "gcp",
        "azure",
        "snowflake_oauth_user_account",
        "snowflake_key_pair_user_account",
        "api_token",
    ],
    **kwargs: Any,
) -> str:
    """Get, replace, or create a credential in DR with requested parameters.

    Notes
    -----
    Records a checksum in credential description field to allow future calls to this
    function to validate whether a desired credential already exists.

    DataRobot credential names must be unique.

    If a credential with the requested name exists but has a different checksum,
    the existing credential will be updated if possible (same credential_type) or deleted if not.
    The new credential is created with the requested parameters. The new credential will bear the same name as
    the deleted one, but it will have a different credential id, which allows downstream processes
    to detect that the credential value may have changed and act appropriately.
    """
    dr.Client(token=token, endpoint=endpoint)  # type: ignore[attr-defined]
    credential_token = get_hash(name, credential_type, **kwargs)
    try:
        return _get_or_update_or_delete_existing_credential(
            name, credential_type, credential_token, **kwargs
        )
    except KeyError:
        credential_id = _create_credential(endpoint, token, name, credential_type, **kwargs)
        credential = dr.Credential.get(credential_id)  # type: ignore[attr-defined]
        credential.update(description=f"Checksum: {credential_token}")
        return credential_id
