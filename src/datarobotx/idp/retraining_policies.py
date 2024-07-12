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

import datarobot as dr


def update_or_create_retraining_policy(
    endpoint: str, token: str, deployment_id: str, name: str, dataset_id: str = None, **kwargs
) -> str:
    """Update or create a retraining policy for a model deployment

    Parameters
    ----------
    name:
        name of the retraining policy
    dataset_id: str
        id of the dataset used for retraining
    kwargs:
        https://docs.datarobot.com/en/docs/api/reference/public-api/deployments.html#schemaretrainingpolicycreate

    Returns
    -------
    id of the created retraining policy
    """
    client = dr.Client(token=token, endpoint=endpoint)

    # Configure retraining settings
    if dataset_id is not None:
        prediction_env_id = dr.models.Deployment.get(deployment_id=deployment_id).default_prediction_server["id"]
        payload = {"datasetId": dataset_id,
                   "predictionEnvironmentId": prediction_env_id}
        client.request(
            method="PATCH", url=f"deployments/{deployment_id}/retrainingSettings", json=payload
        )
    
    response = client.request(method="GET", url=f"deployments/{deployment_id}/retrainingPolicies")
    payload = json.loads(response.text)
    policies = payload["data"]
    # Create payload, how do I add on kwargs here?
    policy_payload = {"name": name}

    if not policies:
        response = client.request(method="POST", 
                       url=f"deployments/{deployment_id}/retrainingPolicies",
                       json=policy_payload)
        print(response.text)
    else:
        print("Patch")

    return True
