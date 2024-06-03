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

from typing import Any, Dict, List, Literal, Tuple

import datarobot as dr
from datarobot.utils import camelize
from datarobot.utils.pagination import unpaginate


def _find_existing_custom_metric(
    endpoint: str, token: str, deployment_id: str, **kwargs: Any
) -> Tuple[str, str]:
    url = f"deployments/{deployment_id}/customMetrics/"
    client = dr.Client(token, endpoint)  # type: ignore[attr-defined]
    for metric in unpaginate(initial_url=url, initial_params=None, client=client):
        if all([metric[camelize(key)] == kwargs[key] for key in kwargs]):
            return str(metric["id"]), "get"
        elif metric["name"] == kwargs["name"]:
            if metric["isModelSpecific"] != kwargs["is_model_specific"]:
                raise ValueError(
                    "Parameter `is_model_specific` cannot be changed on an existing "
                    "custom metric. Consider first deleting the existing metric."
                )
            return str(metric["id"]), "patch"
    raise KeyError("No matching custom metric found")


def _patch_metric(
    endpoint: str,
    token: str,
    deployment_id: str,
    metric_id: str,
    **kwargs: Any,
) -> str:
    """Update an existing custom metric on a deployment."""
    url = f"deployments/{deployment_id}/customMetrics/{metric_id}"
    camelize_kwargs = {camelize(k): v for k, v in kwargs.items()}
    response = dr.Client(token, endpoint).patch(url=url, json=camelize_kwargs).json()  # type: ignore[attr-defined]
    return str(response["id"])


def _make_metric(endpoint: str, token: str, deployment_id: str, **kwargs: Any) -> str:
    """Post a new metric to a deployment."""
    route = f"deployments/{deployment_id}/customMetrics/"
    camelize_kwargs = {camelize(k): v for k, v in kwargs.items()}
    response = dr.Client(token, endpoint).post(url=route, json=camelize_kwargs).json()  # type: ignore[attr-defined]
    return str(response["id"])


def get_update_or_create_custom_metric(
    endpoint: str,
    token: str,
    deployment_id: str,
    name: str,
    directionality: Literal["higherIsBetter", "lowerIsBetter"],
    units: str,
    type: Literal["average", "sum", "guage"],
    baseline_values: List[Dict[str, int]],
    is_model_specific: bool,
    **kwargs: Any,
) -> str:
    """
    Get, update, or create a custom metric with the requested parameters.

    If a metric with the requested name and parameters already exists, return its ID.
    If a metric with the requested name exists with different parameters, it will be patched.
    If no metric with the requested name exists, it will be created.

    Parameters
    ----------
    name : str
        The name of the metric
    directionality : str
        The direction of the metric. Must be one of "higherIsBetter" or "lowerIsBetter"
    units : str
        The units of the metric (what is displayed on the y-axis)
    type : str
        The aggregation type of the metric. Must be one of "average", "sum", or "gauge"
    baseline_values : list of dicts
        The baseline value of the metric
    is_model_specific: bool
        Whether the metric relates to a specific model or the deployment as a whole
    """
    try:
        metric_id, status = _find_existing_custom_metric(
            endpoint,
            token,
            deployment_id,
            name=name,
            directionality=directionality,
            units=units,
            type=type,
            baseline_values=baseline_values,
            is_model_specific=is_model_specific,
            **kwargs,
        )
        if status == "patch":
            _patch_metric(
                endpoint,
                token,
                deployment_id,
                metric_id,
                name=name,
                directionality=directionality,
                units=units,
                type=type,
                baseline_values=baseline_values,
                **kwargs,
            )
        return metric_id
    except KeyError:
        return _make_metric(
            endpoint,
            token,
            deployment_id,
            name=name,
            directionality=directionality,
            units=units,
            type=type,
            is_model_specific=is_model_specific,
            baseline_values=baseline_values,
            **kwargs,
        )
