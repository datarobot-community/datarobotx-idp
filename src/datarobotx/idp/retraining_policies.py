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

import json
from typing import Any, Dict, Union

import datarobot as dr


def _check_response(response: Any) -> None:
    if response.status_code < 200 or response.status_code > 399:
        raise Exception(f"Request failed with status code {response.status_code}: {response.text}")


def update_or_create_retraining_policy(
    endpoint: str,
    token: str,
    deployment_id: str,
    name: str,
    dataset_id: Union[str, None] = None,
    **kwargs: Any,
) -> str:
    """Update or create a retraining policy for a model deployment.

    Parameters
    ----------
    endpoint : str
        The endpoint for the DataRobot API.
    token : str
        The token for authentication with the DataRobot API.
    deployment_id : str
        The ID of the deployment to update or create the retraining policy for.
    name : str
        The name of the retraining policy.
    dataset_id : Union[str, None], optional
        The ID of the dataset used for retraining, by default None.
    **kwargs : Any
        Additional keyword arguments for the retraining policy.

    Returns
    -------
    str
        The ID of the created retraining policy.
    """
    client = dr.Client(token=token, endpoint=endpoint)  # type: ignore

    # Configure retraining settings
    if dataset_id is not None:
        deployment = dr.Deployment.get(deployment_id=deployment_id)  # type: ignore
        prediction_env_id = deployment.default_prediction_server["id"]  # type: ignore
        get_payload = {"datasetId": dataset_id, "predictionEnvironmentId": prediction_env_id}
        settings_response = client.request(
            method="PATCH", url=f"deployments/{deployment_id}/retrainingSettings", json=get_payload
        )
        _check_response(settings_response)

    get_response = client.request(
        method="GET", url=f"deployments/{deployment_id}/retrainingPolicies"
    )
    retraining_policies_payload: Dict[str, Any] = json.loads(get_response.text)
    policies = retraining_policies_payload["data"]

    policy_payload_update = {"name": name, **kwargs}
    response: Any = None
    retraining_policy_id: str = ""

    # If there is an existing policy w/ inputted name, PATCH, otherwise POST
    for policy in policies:
        if policy["name"] == name:
            retraining_policy_id = policy["id"]
            response = client.request(
                method="PATCH",
                url=f"deployments/{deployment_id}/retrainingPolicies/{retraining_policy_id}",
                json=policy_payload_update,
            )
            print(json.loads(response.text))
            break

    if response is None:
        response = client.request(
            method="POST",
            url=f"deployments/{deployment_id}/retrainingPolicies",
            json=policy_payload_update,
        )
        retraining_policy_id = json.loads(response.text)["id"]
        print(json.loads(response.text))

    return retraining_policy_id
