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

from typing import Any, Dict, Optional

import datarobot as dr
from datarobot.models.batch_job import Schedule
from datarobot.models.batch_prediction_job import BatchPredictionJobDefinition


def get_update_or_create_batch_prediction_job(
    endpoint: str,
    token: str,
    deployment_id: str,
    batch_prediction_job: Dict[str, Any],
    enabled: bool,
    name: str,
    schedule: Optional[Schedule],
) -> str:
    """Create or update a batch prediction job definition.

    Parameters
    ----------
    batch_prediction_job: dict
        The job specifications for your batch prediction job.
        See 'datarobot.models.batch_prediction_job.BatchPredictionJobDefinition.create()'
    enabled: bool
        Whether or not the definition should be active on a scheduled basis. If True, `schedule` is required
    name: str
        * Must be unique to your organization *
        Name of batch prediction job definition. If given the name of an existing definition within the supplied
        deployment (according to deployment_id), this function will overwrite that existing definition with parameters
        specified in this function (batch_prediction_job, enabled, schedule).
    schedule : dict (optional)
        The ``schedule`` payload defines at what intervals the job should run, which can be
        combined in various ways to construct complex scheduling terms if needed. In all of
        the elements in the objects, you can supply either an asterisk ``["*"]`` denoting
        "every" time denomination or an array of integers (e.g. ``[1, 2, 3]``) to define
        a specific interval.

    Returns
    -------
    id of the created/updated batch prediction job definition

    """
    dr.Client(token=token, endpoint=endpoint)  # type: ignore

    jobs = BatchPredictionJobDefinition.list(search_name=name, deployment_id=deployment_id, limit=0)
    job: BatchPredictionJobDefinition
    if jobs:
        # There should only be 1 batchpredictionjobdefinition with the same name.
        job = jobs[0]
        job.update(
            enabled=enabled, batch_prediction_job=batch_prediction_job, name=name, schedule=schedule
        )
    else:
        job = BatchPredictionJobDefinition.create(
            enabled=enabled, batch_prediction_job=batch_prediction_job, name=name, schedule=schedule
        )

    return job.id  # type: ignore
