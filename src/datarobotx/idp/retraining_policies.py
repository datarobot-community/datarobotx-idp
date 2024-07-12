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
        payload = client.request(
            method="GET", url=f"deployments/{deployment_id}/retrainingSettings"
        )
        print(type(payload))
        print(json.loads(payload.text))

    return payload
