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
from typing import Any, Union, Dict

import datarobot as dr

def update_or_create_retraining_policy(
    endpoint: str,
    token: str,
    deployment_id: str,
    name: str,
    dataset_id: Union[str, None] = None,
    **kwargs: Any,
) -> None:
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
        deployment = dr.models.Deployment.get(deployment_id=deployment_id)
        prediction_env_id = deployment.default_prediction_server["id"]  # type: ignore
        payload = {"datasetId": dataset_id, "predictionEnvironmentId": prediction_env_id}
        client.request(
            method="PATCH", url=f"deployments/{deployment_id}/retrainingSettings", json=payload
        )

    response = client.request(method="GET", url=f"deployments/{deployment_id}/retrainingPolicies")
    payload: Dict[str, Any] = json.loads(response.text)
    policies = payload["data"]

    policy_payload = {"name": name, **kwargs}
    response = None
    retraining_policy_id = None

    for policy in policies:
        if policy["name"] == name:
            retraining_policy_id = policy["id"]
            response = client.request(
                method="PATCH",
                url=f"deployments/{deployment_id}/retrainingPolicies/{retraining_policy_id}",
                json=policy_payload,
            )
            break  # No need to continue once the policy is found and updated

    if response is None:
        response = client.request(
            method="POST",
            url=f"deployments/{deployment_id}/retrainingPolicies",
            json=policy_payload,
        )
    
    if retraining_policy_id is None:
        response = client.request(method="GET", url=f"deployments/{deployment_id}/retrainingPolicies")
        payload: Dict[str, Any] = json.loads(response.text)
        policies = payload["data"]
        for policy in policies:
            if policy["name"] == name:
                return policy["id"]

    return retraining_policy_id

