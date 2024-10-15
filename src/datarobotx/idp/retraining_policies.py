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
from typing import Any, Optional

import datarobot as dr


def _check_response(response: Any) -> None:
    if response.status_code < 200 or response.status_code > 299:
        raise Exception(f"Request failed with status code {response.status_code}: {response.text}")


def _configure_retraining_settings(
    dataset_id: str,
    deployment_id: str,
    client: Any,
    credential_id: Optional[str] = None,
    prediction_environment_id: Optional[str] = None,
) -> None:
    if prediction_environment_id is None:
        deployment: dr.Deployment = dr.Deployment.get(deployment_id=deployment_id)  # type: ignore
        prediction_environment_id = deployment.default_prediction_server["id"]

    user_info = client.request(method="GET", url="account/info")
    retraining_user_id = json.loads(user_info.text)["uid"]

    get_payload = {
        "datasetId": dataset_id,
        "predictionEnvironmentId": prediction_environment_id,
        "retrainingUserId": retraining_user_id,
    }

    if credential_id:
        get_payload["credentialId"] = credential_id

    settings_response = client.request(
        method="PATCH",
        url=f"deployments/{deployment_id}/retrainingSettings",
        json=get_payload,
    )
    _check_response(settings_response)


def _search_for_existing_policy(client: Any, deployment_id: str, policy_name: str) -> Any:
    get_response = client.request(
        method="GET", url=f"deployments/{deployment_id}/retrainingPolicies"
    )
    retraining_policies = json.loads(get_response.text)["data"]

    for policy in retraining_policies:
        if policy["name"] == policy_name:
            return policy["id"]
    return ""


def _post(client: Any, deployment_id: str, payload: Any) -> Any:
    response = client.request(
        method="POST",
        url=f"deployments/{deployment_id}/retrainingPolicies",
        json=payload,
    )
    _check_response(response)
    return json.loads(response.text)["id"]


def _patch(client: Any, deployment_id: str, payload: Any, retraining_policy_id: str) -> None:
    response = client.request(
        method="PATCH",
        url=f"deployments/{deployment_id}/retrainingPolicies/{retraining_policy_id}",
        json=payload,
    )
    _check_response(response)
    return None


def get_update_or_create_retraining_policy(
    endpoint: str,
    token: str,
    deployment_id: str,
    name: str,
    dataset_id: Optional[str] = None,
    prediction_environment_id: Optional[str] = None,
    **kwargs: Any,
) -> str:
    """Update or create a retraining policy for a model deployment.

    Parameters
    ----------
    dataset_id : Optional
        The ID of the dataset used for retraining, by default None.
        Inputting the dataset triggers an update of the retrainingSettings,
            which causes the first owner of the deployment to be set as the
            policy owner of the retraining policy (within retraining settings)
    retraining_user_id: Optional
        The ID of the user that will be tied to the retraining policy
        Will only be used if dataset_id is supplied.
    **kwargs : Any
        Additional keyword arguments for the retraining policy.
        Reference: https://docs.datarobot.com/en/docs/api/reference/public-api/deployments.html#patch-apiv2deploymentsdeploymentidretrainingpoliciesretrainingpolicyid

    Returns
    -------
    str
        The ID of the created retraining policy.
    """
    client = dr.Client(token=token, endpoint=endpoint)  # type: ignore

    credential_id = kwargs.pop("credential_id", None)

    if dataset_id:
        _configure_retraining_settings(
            dataset_id=dataset_id,
            deployment_id=deployment_id,
            client=client,
            credential_id=credential_id,
            prediction_environment_id=prediction_environment_id,
        )

    policy_payload_to_upload = {"name": name, **kwargs}

    retraining_policy_id: str = _search_for_existing_policy(client, deployment_id, name)

    # If there is no existing policy w/ inputted name, POST, otherwise PATCH
    if retraining_policy_id == "":
        retraining_policy_id = _post(client, deployment_id, policy_payload_to_upload)
    else:
        _patch(client, deployment_id, policy_payload_to_upload, retraining_policy_id)

    return retraining_policy_id
