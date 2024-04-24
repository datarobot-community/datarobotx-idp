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

from typing import Any, Dict, Union

from .common.hashing import get_hash

import datarobot as dr


def _find_existing_datasource(canonical_name: str) -> str:
    dss = dr.DataSource.list()  # type: ignore
    datasource = [ds for ds in dss if ds.canonical_name == canonical_name][0]
    return str(datasource.id)


def get_or_create_datasource(
    endpoint: str,
    token: str,
    data_source_type: str,
    canonical_name: str,
    params: Union[Dict[str, Any], dr.DataSourceParameters],  # type: ignore
) -> str:
    """Get or create a DR datasource with requested parameters.

    Notes
    -----
    Records a checksum in the canonical name to allow future calls to this
    function to validate whether a desired datasource already exists
    """
    dr.Client(token=token, endpoint=endpoint)  # type: ignore

    if isinstance(params, dr.DataSourceParameters):  # type: ignore
        params_obj = params
    else:
        params_obj = dr.DataSourceParameters(**params)  # type: ignore

    datasource_token = get_hash(data_source_type, canonical_name, params_obj.collect_payload())
    canonical_name = f"{canonical_name} [{datasource_token}]"

    try:
        return _find_existing_datasource(canonical_name)
    except IndexError:
        pass

    datasource = dr.DataSource.create(  # type: ignore
        data_source_type=data_source_type,
        canonical_name=canonical_name,
        params=params_obj,
    )
    return str(datasource.id)
