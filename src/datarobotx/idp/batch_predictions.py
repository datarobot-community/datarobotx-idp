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

from typing import Optional, TypedDict

import datarobot as dr
from datarobot.models.batch_job import IntakeSettings, OutputSettings, Schedule
from datarobot.models.batch_prediction_job import BatchPredictionJobDefinition


class JobSpec(TypedDict):
    """Jobspec type definition.

    num_concurrent : int (optional)
        Number of concurrent chunks to score simultaneously. Defaults to
        the available number of cores of the deployment. Lower it to leave
        resources for real-time scoring.
    intake_settings : dict (optional)
        A dict configuring how data is coming from.

         A dict configuring how data is coming from. Supported options:

                - type : string, either `localFile`, `s3`, `azure`, `gcp`, `dataset`, `jdbc`
                  `snowflake`, `synapse` or `bigquery`

            Note that to pass a dataset, you not only need to specify the `type` parameter
            as `dataset`, but you must also set the `dataset` parameter as a
            `dr.Dataset` object.

            To score from a local file, add the this parameter to the
            settings:

                - file : file-like object, string path to file or a
                  pandas.DataFrame of scoring data

            To score from S3, add the next parameters to the settings:

                - url : string, the URL to score (e.g.: `s3://bucket/key`)
                - credential_id : string (optional)
                - endpoint_url : string (optional), any non-default endpoint
                  URL for S3 access (omit to use the default)

            .. _batch_predictions_jdbc_creds_usage:

            To score from JDBC, add the next parameters to the settings:

                - data_store_id : string, the ID of the external data store connected
                  to the JDBC data source (see
                  :ref:`Database Connectivity <database_connectivity_overview>`).
                - query : string (optional if `table`, `schema` and/or `catalog` is specified),
                  a self-supplied SELECT statement of the data set you wish to predict.
                - table : string (optional if `query` is specified),
                  the name of specified database table.
                - schema : string (optional if `query` is specified),
                  the name of specified database schema.
                - catalog : string  (optional if `query` is specified),
                  (new in v2.22) the name of specified database catalog.
                - fetch_size : int (optional),
                  Changing the `fetchSize` can be used to balance throughput and memory
                  usage.
                - credential_id : string (optional) the ID of the credentials holding
                  information about a user with read-access to the JDBC data source (see
                  :ref:`Credentials <credentials_api_doc>`).

    output_settings : dict (optional)
        A dict configuring how scored data is to be saved. Supported
            options:

                - type : string, either `localFile`, `s3`, `azure`, `gcp`, `jdbc`,
                  `snowflake`, `synapse` or `bigquery`

            To save scored data to a local file, add this parameters to the
            settings:

                - path : string (optional), path to save the scored data
                  as CSV. If a path is not specified, you must download
                  the scored data yourself with `job.download()`.
                  If a path is specified, the call will block until the
                  job is done. if there are no other jobs currently
                  processing for the targeted prediction instance,
                  uploading, scoring, downloading will happen in parallel
                  without waiting for a full job to complete. Otherwise,
                  it will still block, but start downloading the scored
                  data as soon as it starts generating data. This is the
                  fastest method to get predictions.

            To save scored data to S3, add the next parameters to the settings:

                - url : string, the URL for storing the results
                  (e.g.: `s3://bucket/key`)
                - credential_id : string (optional)
                - endpoint_url : string (optional), any non-default endpoint
                  URL for S3 access (omit to use the default)

            To save scored data to JDBC, add the next parameters to the settings:

                - `data_store_id` : string, the ID of the external data store connected to
                  the JDBC data source (see
                  :ref:`Database Connectivity <database_connectivity_overview>`).
                - `table` : string,  the name of specified database table.
                - `schema` : string (optional), the name of specified database schema.
                - `catalog` : string (optional), (new in v2.22) the name of specified database
                  catalog.
                - `statement_type` : string, the type of insertion statement to create,
                  one of ``datarobot.enums.AVAILABLE_STATEMENT_TYPES``.
                - `update_columns` : list(string) (optional),  a list of strings containing
                  those column names to be updated in case `statement_type` is set to a
                  value related to update or upsert.
                - `where_columns` : list(string) (optional), a list of strings containing
                  those column names to be selected in case `statement_type` is set to a
                  value related to insert or update.
                - `credential_id` : string, the ID of the credentials holding information about
                  a user with write-access to the JDBC data source (see
                  :ref:`Credentials <credentials_api_doc>`).
                - `create_table_if_not_exists` : bool (optional), If no existing table is detected,
                  attempt to create it before writing data with the strategy defined in the
                  statementType parameter.
    """

    num_concurrent: Optional[int]
    deployment_id: str
    intake_settings: IntakeSettings
    output_settings: Optional[OutputSettings]


def update_or_create_batch_prediction_job(
    endpoint: str,
    token: str,
    deployment_id: str,
    batch_prediction_job: JobSpec,
    enabled: bool,
    name: str,
    schedule: Optional[Schedule],
) -> str | None:
    """Create or update a batch prediction job definition.

    Parameters
    ----------
    batch_prediction_job: JobSpec
        The job specifications for your batch prediction job. See the TypedDict above for specific inputs
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
        See specifics: https://github.com/datarobot/public_api_client/blob/master/datarobot/models/batch_prediction_job.py#L1872

    Returns
    -------
    id of the created/updated batch prediction job definition

    """
    dr.Client(token=token, endpoint=endpoint)  # type: ignore

    jobs = BatchPredictionJobDefinition.list(search_name=name, deployment_id=deployment_id, limit=0)

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

    return job.id
