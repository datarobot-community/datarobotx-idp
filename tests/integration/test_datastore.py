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

import pytest

from datarobotx.idp.datastore import get_or_create_datastore


@pytest.fixture
def canonical_name_1():
    return "pytest datastore"


@pytest.fixture
def canonical_name_2():
    return "pytest datastore 2"


@pytest.fixture
def cleanup_env(cleanup_dr, debug_override):
    with cleanup_dr("externalDataStores/", debug_override):
        yield


def test_get_or_create(
    dr_endpoint,
    dr_token,
    canonical_name_1,
    canonical_name_2,
    snowflake_driver_id,
    snowflake_jdbc_url,
    cleanup_env,
):
    data_store_id_1 = get_or_create_datastore(
        endpoint=dr_endpoint,
        token=dr_token,
        data_store_type="jdbc",
        canonical_name=canonical_name_1,
        driver_id=snowflake_driver_id,
        jdbc_url=snowflake_jdbc_url,
    )
    assert len(data_store_id_1)

    data_store_id_2 = get_or_create_datastore(
        endpoint=dr_endpoint,
        token=dr_token,
        data_store_type="jdbc",
        canonical_name=canonical_name_1,
        driver_id=snowflake_driver_id,
        jdbc_url=snowflake_jdbc_url,
    )
    assert data_store_id_1 == data_store_id_2

    data_store_id_3 = get_or_create_datastore(
        endpoint=dr_endpoint,
        token=dr_token,
        data_store_type="jdbc",
        canonical_name=canonical_name_2,
        driver_id=snowflake_driver_id,
        jdbc_url=snowflake_jdbc_url,
    )
    assert data_store_id_1 != data_store_id_3
