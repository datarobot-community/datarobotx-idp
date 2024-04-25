import pandas as pd
import pytest

import datarobot as dr
from datarobotx.idp.credentials import get_replace_or_create_credential
from datarobotx.idp.datasets import (
    get_or_create_dataset_from_datasource,
    get_or_create_dataset_from_df,
    get_or_create_dataset_from_file,
)
from datarobotx.idp.datasource import get_or_create_datasource
from datarobotx.idp.datastore import get_or_create_datastore
from datarobotx.idp.use_cases import get_or_create_use_case


@pytest.fixture
def use_case(dr_endpoint, dr_token, cleanup_dr):
    with cleanup_dr("useCases/"):
        yield get_or_create_use_case(
            dr_endpoint,
            dr_token,
            "pytest use case",
        )


@pytest.fixture
def cleanup_env(cleanup_dr):
    with cleanup_dr("datasets/", id_attribute="datasetId"):
        yield


@pytest.fixture
def dummy_df(tmp_path):
    return pd.DataFrame(
        {
            "num_legs": [2, 4, 8, 0],
            "num_wings": [2, 0, 0, 0],
            "num_specimen_seen": [10, 2, 1, 8],
        },
        index=["falcon", "dog", "spider", "fish"],
    )


@pytest.fixture
def dummy_dataset_file(tmp_path, dummy_df):
    path = tmp_path / "py_test.csv"
    dummy_df.to_csv(path)
    return str(path)


@pytest.fixture
def cleanup_datastore(cleanup_dr):
    with cleanup_dr("externalDataStores/"):
        yield


@pytest.fixture
def cleanup_datasource(cleanup_dr):
    with cleanup_dr("externalDataSources/"):
        yield


@pytest.fixture
def cleanup_credentials(cleanup_dr):
    with cleanup_dr("credentials/", id_attribute="credentialId"):
        yield


@pytest.fixture
def data_store_id(dr_token, dr_endpoint, snowflake_driver_id, snowflake_jdbc_url, cleanup_datastore):
    return get_or_create_datastore(
        endpoint=dr_endpoint,
        token=dr_token,
        data_store_type='jdbc',
        canonical_name="pytest_datastore",
        driver_id=snowflake_driver_id,
        jdbc_url=snowflake_jdbc_url,
    )


@pytest.fixture
def snowflake_credentials(
    dr_endpoint, dr_token, snowflake_user, snowflake_password, cleanup_credentials
):
    return get_replace_or_create_credential(
        endpoint=dr_endpoint,
        token=dr_token,
        name="pytest snowflake credentials",
        credential_type="basic",
        user=snowflake_user,
        password=snowflake_password,
    )


@pytest.fixture
def data_source_id(
    data_store_id, snowflake_training_table_name, cleanup_datasource, dr_token, dr_endpoint
):
    return get_or_create_datasource(
        endpoint=dr_endpoint,
        token=dr_token,
        data_source_type='jdbc',
        canonical_name="pytest datastore",
        params=dr.DataSourceParameters(
            data_store_id=data_store_id,
            **snowflake_training_table_name,
        ),
    )


def test_get_or_create_from_file(dr_endpoint, dr_token, use_case, dummy_dataset_file, cleanup_env):
    use_case_id_1 = get_or_create_dataset_from_file(
        dr_endpoint,
        dr_token,
        use_case,
        "pytest dataset #1",
        dummy_dataset_file,
    )
    assert len(use_case_id_1)

    use_case_id_2 = get_or_create_dataset_from_file(
        dr_endpoint,
        dr_token,
        use_case,
        "pytest dataset #1",
        dummy_dataset_file,
    )
    assert use_case_id_1 == use_case_id_2

    use_case_id_3 = get_or_create_dataset_from_file(
        dr_endpoint,
        dr_token,
        use_case,
        "pytest dataset #2",
        dummy_dataset_file,
    )
    assert use_case_id_1 != use_case_id_3


def test_get_or_create_from_df(dr_endpoint, dr_token, use_case, dummy_df, cleanup_env):
    use_case_id_1 = get_or_create_dataset_from_df(
        dr_endpoint,
        dr_token,
        use_case,
        "pytest dataset #1",
        dummy_df,
    )
    assert len(use_case_id_1)

    use_case_id_2 = get_or_create_dataset_from_df(
        dr_endpoint,
        dr_token,
        use_case,
        "pytest dataset #1",
        dummy_df,
    )
    assert use_case_id_1 == use_case_id_2

    use_case_id_3 = get_or_create_dataset_from_df(
        dr_endpoint,
        dr_token,
        use_case,
        "pytest dataset #2",
        dummy_df,
    )
    assert use_case_id_1 != use_case_id_3


def test_get_or_create_from_datasource(
    dr_endpoint, dr_token, data_source_id, use_case, snowflake_credentials, cleanup_env
):
    use_case_id_1 = get_or_create_dataset_from_datasource(
        token=dr_token,
        endpoint=dr_endpoint,
        name="pytest dataset #1",
        data_source_id=data_source_id,
        use_case_id=use_case,
        credential_id=snowflake_credentials,
    )
    assert len(use_case_id_1)

    use_case_id_2 = get_or_create_dataset_from_datasource(
        token=dr_token,
        endpoint=dr_endpoint,
        name="pytest dataset #1",
        data_source_id=data_source_id,
        use_case_id=use_case,
        credential_id=snowflake_credentials,
    )
    assert use_case_id_1 == use_case_id_2

    use_case_id_3 = get_or_create_dataset_from_datasource(
        token=dr_token,
        endpoint=dr_endpoint,
        name="pytest dataset #2",
        data_source_id=data_source_id,
        use_case_id=use_case,
        credential_id=snowflake_credentials,
    )
    assert use_case_id_1 != use_case_id_3
