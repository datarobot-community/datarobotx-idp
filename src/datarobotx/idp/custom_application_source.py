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


import datarobot as dr


def get_or_create_custom_application_source(endpoint: str, token: str, name: str) -> str:
    """
    Get or create a custom application source with the given name.

    Parameters
    ----------
    name : str
        The name of the custom application source.

    Returns
    -------
    str
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
