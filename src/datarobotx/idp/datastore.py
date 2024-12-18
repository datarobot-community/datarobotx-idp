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


from typing import Any

from .common.hashing import get_hash

import datarobot as dr


def _find_existing_datastore(canonical_name: str) -> str:
    dss = dr.DataStore.list(typ="all")  # type: ignore
    datastore = [
        ds for ds in dss if ds.canonical_name is not None and ds.canonical_name == canonical_name
    ][0]
    return str(datastore.id)


def get_or_create_datastore(
    endpoint: str,
    token: str,
    data_store_type: str,
    canonical_name: str,
    driver_id: str,
    **kwargs: Any,
) -> str:
    """Get or create a DR datastore with requested parameters.

    Notes
    -----
    Records a checksum in the datasource name to allow future calls to this
    function to validate whether a desired datasource already exists
    """
    dr.Client(token=token, endpoint=endpoint)  # type: ignore

    datastore_token = get_hash(canonical_name, driver_id, data_store_type, **kwargs)

    canonical_name = f"{canonical_name} [{datastore_token}]"
    try:
        return _find_existing_datastore(canonical_name)
    except IndexError:
        pass

    datastore = dr.DataStore.create(  # type: ignore
        data_store_type=data_store_type,
        canonical_name=canonical_name,
        driver_id=driver_id,
        **kwargs,
    )
    return str(datastore.id)
