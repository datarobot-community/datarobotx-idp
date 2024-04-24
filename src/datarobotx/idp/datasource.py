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


from .common.hashing import get_hash

import datarobot as dr


def _find_existing_datasource(canonical_name: str) -> str:
    dss = dr.DataSource.list()  # type: ignore
    datasource = [ds for ds in dss if ds.canonical_name == canonical_name][0]
    return str(datasource.id)


def get_or_create_datasource(
    endpoint: str,
    token: str,
    canonical_name: str,
    data_store_id: str,
    table: str | None = None,
    schema: str | None = None,
    partition_column: str | None = None,
    query: str | None = None,
    fetch_size: int | None = None,
    data_source_type: str = "jdbc",
) -> str:
    """Get or create a DR datasource with requested parameters.

    Notes
    -----
    Records a checksum in the datasource name to allow future calls to this
    function to validate whether a desired datasource already exists
    """
    dr.Client(token=token, endpoint=endpoint)  # type: ignore
    # hash all input params
    datasource_token = get_hash(
        canonical_name,
        data_store_id,
        table,
        schema,
        partition_column,
        query,
        fetch_size,
        data_source_type,
    )
    datasource_name = f"{canonical_name} [{datasource_token}]"
    try:
        datasource = dr.DataSource.get(_find_existing_datasource(datasource_name))  # type: ignore
        return str(datasource.id)
    except IndexError:
        pass

    datasource_params = dr.DataSourceParameters(  # type: ignore
        data_store_id=data_store_id,
        table=table,
        schema=schema,
        partition_column=partition_column,
        query=query,
        fetch_size=fetch_size,
    )
    datasource = dr.DataSource.create(  # type: ignore
        canonical_name=datasource_name,
        params=datasource_params,
        data_source_type=data_source_type,
    )
    return str(datasource.id)
