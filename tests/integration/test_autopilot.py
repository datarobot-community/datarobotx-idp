#
# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# DataRobot, Inc.
#
# This is proprietary source code of DataRobot, Inc. and its
# affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
import uuid

import pandas as pd
import pytest

import datarobot as dr
from datarobotx.idp.autopilot import get_or_create_autopilot_run
import datarobotx.idp.common.hashing
from datarobotx.idp.datasets import get_or_create_dataset_from_df
from datarobotx.idp.use_cases import get_or_create_use_case


@pytest.fixture
def identifier():
    return str(uuid.uuid4())[:6]


@pytest.fixture()
def mock_get_hash(monkeypatch, identifier):
    """Mock hasher. Always return the same hash."""

    def _mock_get_hash(*args, **kwargs):
        return identifier

    monkeypatch.setattr(datarobotx.idp.common.hashing, "get_hash", _mock_get_hash)
    monkeypatch.setattr("datarobotx.idp.autopilot.get_hash", _mock_get_hash)
    return


@pytest.fixture
def analyze_and_model_config():
    return {
        "target": "SalesCount",
        "mode": "quick",
        "metric": "RMSE",
        "worker_count": -1,
    }


@pytest.fixture
def datetime_partitioning_config():
    return {
        "datetime_partition_column": "Date",
        "use_time_series": True,
        "forecast_window_start": 1,
        "forecast_window_end": 7,
    }


@pytest.fixture()
def feature_settings_config():
    return [
        {"feature_name": "AprioriHoliday", "known_in_advance": True},
        {"feature_name": "ClicksLicensedLender", "do_not_derive": True},
    ]


@pytest.fixture
def advanced_options_config():
    return {"seed": 42}


@pytest.fixture
def use_case(dr_endpoint, dr_token, cleanup_dr):
    with cleanup_dr("useCases/"):
        yield get_or_create_use_case(
            dr_endpoint,
            dr_token,
            "pytest use case",
        )


@pytest.fixture
def df():
    return pd.read_csv(
        "https://s3.amazonaws.com/datarobot_public_datasets/DR_Demo_Google_AdWords.csv"
    ).loc[lambda x: x.Date <= "2017-10-20"]


@pytest.fixture
def dataset(dr_endpoint, dr_token, df, use_case, cleanup_dr):
    with cleanup_dr("datasets/", id_attribute="datasetId"):
        yield get_or_create_dataset_from_df(
            dr_endpoint,
            dr_token,
            use_case_id=use_case,
            name="pytest_test_autopilot_dataset",
            data_frame=df,
        )


@pytest.fixture
def cleanup_projects(cleanup_dr):
    with cleanup_dr("projects/", paginated=False):
        yield


@pytest.fixture
def partly_configured_dr_project(
    dataset,
    datetime_partitioning_config,
    advanced_options_config,
    mock_get_hash,
    cleanup_projects,
):
    project_name = "pytest partial"
    project_config_token = datarobotx.idp.common.hashing.get_hash()

    project_name = f"{project_name} [{project_config_token}]"

    project = dr.Project.create_from_dataset(  # type: ignore
        dataset_id=dataset, project_name=project_name
    )
    project.set_datetime_partitioning(**datetime_partitioning_config)
    project.set_options(dr.AdvancedOptions(**advanced_options_config))

    return project


def test_get_or_create(
    dr_endpoint,
    dr_token,
    dataset,
    analyze_and_model_config,
    datetime_partitioning_config,
    feature_settings_config,
    advanced_options_config,
    use_case,
    cleanup_projects,
):
    project_id_1 = get_or_create_autopilot_run(
        dr_endpoint,
        dr_token,
        "pytest autopilot",
        dataset,
        analyze_and_model_config=analyze_and_model_config,
        datetime_partitioning_config=datetime_partitioning_config,
        feature_settings_config=feature_settings_config,
        advanced_options_config=advanced_options_config,
        use_case=use_case,
    )
    assert len(project_id_1)

    project_id_2 = get_or_create_autopilot_run(
        dr_endpoint,
        dr_token,
        "pytest autopilot",
        dataset,
        analyze_and_model_config=analyze_and_model_config,
        datetime_partitioning_config=datetime_partitioning_config,
        feature_settings_config=feature_settings_config,
        advanced_options_config=advanced_options_config,
        use_case=use_case,
    )

    assert project_id_1 == project_id_2

    advanced_options_config["seed"] = 43

    project_id_3 = get_or_create_autopilot_run(
        dr_endpoint,
        dr_token,
        "pytest autopilot",
        dataset,
        analyze_and_model_config=analyze_and_model_config,
        datetime_partitioning_config=datetime_partitioning_config,
        feature_settings_config=feature_settings_config,
        advanced_options_config=advanced_options_config,
        use_case=use_case,
    )

    assert project_id_1 != project_id_3


def test_get_or_create_partly_configured(
    dr_endpoint,
    dr_token,
    dataset,
    analyze_and_model_config,
    datetime_partitioning_config,
    advanced_options_config,
    mock_get_hash,
    partly_configured_dr_project,
    cleanup_projects,
):
    project_id = get_or_create_autopilot_run(
        dr_endpoint,
        dr_token,
        "pytest partial",
        dataset,
        analyze_and_model_config=analyze_and_model_config,
        datetime_partitioning_config=datetime_partitioning_config,
        advanced_options_config=advanced_options_config,
    )
    assert len(project_id)
