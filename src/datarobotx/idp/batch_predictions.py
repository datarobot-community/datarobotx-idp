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

from typing import TypedDict, Optional

import datarobot as dr

from datarobot.models.batch_prediction_job import BatchPredictionJobDefinition
from datarobot.models.batch_job import IntakeSettings, OutputSettings, Schedule

class JobSpec(TypedDict):
    """
    num_concurrent : int (optional)
            Number of concurrent chunks to score simultaneously. Defaults to
            the available number of cores of the deployment. Lower it to leave
            resources for real-time scoring.
    intake_settings : dict (optional)
            A dict configuring how data is coming from. 
            
            See supported options: https://github.com/datarobot/public_api_client/blob/master/datarobot/models/batch_prediction_job.py#L429

    output_settings : dict (optional)
            A dict configuring how scored data is to be saved. 
            
            See supported options: https://github.com/datarobot/public_api_client/blob/master/datarobot/models/batch_prediction_job.py#L474

    """
    num_concurrent: Optional[int]
    deployment_id: str # is this rquired
    intake_settings: IntakeSettings
    output_settings: Optional[OutputSettings]

def get_update_or_create_batch_prediction_job(
    endpoint: str,
    token: str,
    deployment_id: str,
    batch_prediction_job: JobSpec,
    enabled: bool, 
    name = str, 
    schedule = Optional[Schedule]
) -> str:
    """Create or update a batch prediction job definition

    Parameters
    ----------
    batch_prediction_job: JobSpec
        The job specifications for your batch prediction job. See the TypedDict above for specific inputs
    enabled: bool
        Whether or not the definition should be active on a scheduled basis. If True, `schedule` is required
    name: str
        Name of batch prediction job definition. If given the name of an existing definition within the supplied
        deployment (according to deployment_id), this function will overwrite that existing definition with parameters
        specified in this function (batch_prediction_job, enabled, schedule)
    schedule : dict (optional)
        The ``schedule`` payload defines at what intervals the job should run, which can be
        combined in various ways to construct complex scheduling terms if needed. In all of
        the elements in the objects, you can supply either an asterisk ``["*"]`` denoting
        "every" time denomination or an array of integers (e.g. ``[1, 2, 3]``) to define
        a specific interval.
        See specifics: https://github.com/datarobot/public_api_client/blob/master/datarobot/models/batch_prediction_job.py#L1872
    
    Returns
    -------
    id of the created/updated batch prediction job definition

    """
    dr.Client(token=token, endpoint=endpoint)

    jobs = BatchPredictionJobDefinition.list(search_name=name, deployment_id=deployment_id, limit=0)

    if jobs:
        # There should only be 1 batchpredictionjobdefinition with the same name.
        job = jobs[0]
        job.update(enabled=enabled, batch_prediction_job=batch_prediction_job, name=name, schedule=schedule)
    else:
        job = BatchPredictionJobDefinition.create(enabled=enabled, batch_prediction_job=batch_prediction_job, name=name, schedule=schedule)

    return job.id