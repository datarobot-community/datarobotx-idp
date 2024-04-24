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

import datarobot as dr
from datarobotx.idp.datasource import get_or_create_datasource
from datarobotx.idp.datastore import get_or_create_datastore


@pytest.fixture
def canonical_name_1():
    return "pytest datasource 1"


@pytest.fixture
def canonical_name_2():
    return "pytest datasource 2"


@pytest.fixture
def cleanup_datastore(cleanup_dr):
    with cleanup_dr("externalDataStores/"):
        yield


@pytest.fixture
def data_store_id(
    dr_token, dr_endpoint, snowflake_driver_id, snowflake_jdbc_url, cleanup_datastore
):
    return get_or_create_datastore(
        token=dr_token,
        endpoint=dr_endpoint,
        data_store_type="jdbc",
        canonical_name="pytest_datastore",
        driver_id=snowflake_driver_id,
        jdbc_url=snowflake_jdbc_url,
    )


@pytest.fixture
def cleanup_datasource(cleanup_dr):
    with cleanup_dr("externalDataSources/"):
        yield


def test_get_or_create(
    dr_endpoint,
    dr_token,
    canonical_name_1,
    canonical_name_2,
    snowflake_training_table_name,
    snowflake_scoring_table_name,
    data_store_id,
    cleanup_datasource,
):
    data_store_id_1 = get_or_create_datasource(
        token=dr_token,
        endpoint=dr_endpoint,
        data_source_type="jdbc",
        canonical_name=canonical_name_1,
        params=dr.DataSourceParameters(
            data_store_id=data_store_id,
            **snowflake_training_table_name,
        ),
    )
    assert len(data_store_id_1)

    data_store_id_2 = get_or_create_datasource(
        token=dr_token,
        endpoint=dr_endpoint,
        data_source_type="jdbc",
        canonical_name=canonical_name_1,
        params=dr.DataSourceParameters(
            data_store_id=data_store_id,
            **snowflake_training_table_name,
        ),
    )
    assert data_store_id_1 == data_store_id_2

    data_store_id_3 = get_or_create_datasource(
        token=dr_token,
        endpoint=dr_endpoint,
        data_source_type="jdbc",
        canonical_name=canonical_name_2,
        params=dr.DataSourceParameters(
            data_store_id=data_store_id,
            **snowflake_scoring_table_name,
        ),
    )
    assert data_store_id_1 != data_store_id_3

    data_store_id_4 = get_or_create_datasource(
        token=dr_token,
        endpoint=dr_endpoint,
        data_source_type="jdbc",
        canonical_name=canonical_name_1,
        params=dr.DataSourceParameters(
            data_store_id=data_store_id,
            **snowflake_scoring_table_name,
        ),
    )
    assert data_store_id_3 != data_store_id_4
