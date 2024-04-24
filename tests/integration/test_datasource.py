import pytest

from datarobotx.idp.datasource import get_or_create_datasource
from datarobotx.idp.datastore import get_or_create_datastore


@pytest.fixture
def canonical_name_1():
    return "pytest datasource 1"


@pytest.fixture
def canonical_name_2():
    return "pytest datasource 2"


@pytest.fixture
def snowflake_driver_id():
    return "6409af3ae3ad5839b69a4daa"


@pytest.fixture
def cleanup_datastore(cleanup_dr):
    with cleanup_dr("externalDataStores/"):
        yield


@pytest.fixture
def snowflake_table_name_1():
    return {"table": "LENDING_CLUB_10K", "schema": "TRAINING"}


@pytest.fixture
def snowflake_table_name_2():
    return {"table": "LENDING_CLUB_10K", "schema": "SCORING"}


@pytest.fixture
def jdbc_url():
    return (
        "jdbc:snowflake://{account}.snowflakecomputing.com/?warehouse={warehouse}&db={db}".format(
            account="datarobot_partner", warehouse="DEMO_WH", db="SANDBOX"
        )
    )


@pytest.fixture
def data_store_id(dr_token, dr_endpoint, snowflake_driver_id, jdbc_url, cleanup_datastore):
    return get_or_create_datastore(
        token=dr_token,
        endpoint=dr_endpoint,
        canonical_name="pytest_datastore",
        driver_id=snowflake_driver_id,
        jdbc_url=jdbc_url,
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
    snowflake_table_name_1,
    snowflake_table_name_2,
    data_store_id,
    cleanup_datasource,
):
    data_store_id_1 = get_or_create_datasource(
        canonical_name=canonical_name_1,
        token=dr_token,
        endpoint=dr_endpoint,
        data_store_id=data_store_id,
        **snowflake_table_name_1,
    )
    assert len(data_store_id_1)

    data_store_id_2 = get_or_create_datasource(
        canonical_name=canonical_name_1,
        token=dr_token,
        endpoint=dr_endpoint,
        data_store_id=data_store_id,
        **snowflake_table_name_1,
    )
    assert data_store_id_1 == data_store_id_2

    data_store_id_3 = get_or_create_datasource(
        canonical_name=canonical_name_2,
        token=dr_token,
        endpoint=dr_endpoint,
        data_store_id=data_store_id,
        **snowflake_table_name_2,
    )
    assert data_store_id_1 != data_store_id_3

    data_store_id_4 = get_or_create_datasource(
        canonical_name=canonical_name_1,
        token=dr_token,
        endpoint=dr_endpoint,
        data_store_id=data_store_id,
        **snowflake_table_name_2,
    )
    assert data_store_id_3 != data_store_id_4
