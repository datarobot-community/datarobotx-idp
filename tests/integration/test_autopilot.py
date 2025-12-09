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

import asyncio
from copy import deepcopy
import uuid

import pandas as pd
import pytest

import datarobot as dr
from datarobotx.idp.autopilot import (
    get_or_create_autopilot_run,
    get_or_create_autopilot_run_async,
)
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


@pytest.fixture()
def calendar_dataset(dr_endpoint, dr_token, use_case, cleanup_dr):
    holidays = ["Thanksgiving", "IndependenceDay", "BlackFriday"]
    years = ["2016", "2017"]
    dates = ["{}-11-26", "{}-07-04", "{}-11-25"]
    df = pd.DataFrame(
        {
            "Date": [date.format(year) for year in years for date in dates],
            "Holiday": holidays * 2,
        }
    )
    with cleanup_dr("datasets/", id_attribute="datasetId"):
        yield get_or_create_dataset_from_df(
            dr_endpoint,
            dr_token,
            use_case_id=use_case,
            name="pytest_test_calendar",
            data_frame=df,
        )


@pytest.fixture
def calendar_from_dataset(dr_endpoint, dr_token, calendar_dataset):
    return dr.CalendarFile.create_calendar_from_dataset(
        calendar_dataset, calendar_name="test_calendar"
    ).id


@pytest.fixture
def cleanup_projects(cleanup_dr):
    with cleanup_dr("projects/", paginated=False):
        yield


@pytest.fixture
def partly_configured_dr_project(
    dataset,
    mock_get_hash,
    cleanup_projects,
):
    project_name = "pytest partial"
    project_config_token = datarobotx.idp.common.hashing.get_hash()

    project_name = f"{project_name} [{project_config_token}]"

    project = dr.Project.create_from_dataset(  # type: ignore
        dataset_id=dataset, project_name=project_name
    )

    return project


def test_get_or_create_autopilot_run(
    dr_endpoint,
    dr_token,
    dataset,
    analyze_and_model_config,
    datetime_partitioning_config,
    feature_settings_config,
    advanced_options_config,
    use_case,
    calendar_from_dataset,
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
        calendar_id=calendar_from_dataset,
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
        calendar_id=calendar_from_dataset,
    )

    assert project_id_1 == project_id_2

    analyze_and_model_config_max_wait = deepcopy(analyze_and_model_config)
    analyze_and_model_config_max_wait["max_wait"] = 1234

    project_id_2_max_wait = get_or_create_autopilot_run(
        dr_endpoint,
        dr_token,
        "pytest autopilot",
        dataset,
        analyze_and_model_config=analyze_and_model_config_max_wait,
        datetime_partitioning_config=datetime_partitioning_config,
        feature_settings_config=feature_settings_config,
        advanced_options_config=advanced_options_config,
        use_case=use_case,
        calendar_id=calendar_from_dataset,
    )

    assert project_id_1 == project_id_2_max_wait

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
        calendar_id=calendar_from_dataset,
    )

    assert project_id_1 != project_id_3


def test_get_or_create_autopilot_run_async(
    dr_endpoint,
    dr_token,
    dataset,
    analyze_and_model_config,
    datetime_partitioning_config,
    feature_settings_config,
    advanced_options_config,
    use_case,
    calendar_from_dataset,
    cleanup_projects,
):
    project_id_1 = asyncio.run(
        get_or_create_autopilot_run_async(
            dr_endpoint,
            dr_token,
            "pytest autopilot",
            dataset,
            analyze_and_model_config=analyze_and_model_config,
            datetime_partitioning_config=datetime_partitioning_config,
            feature_settings_config=feature_settings_config,
            advanced_options_config=advanced_options_config,
            use_case=use_case,
            calendar_id=calendar_from_dataset,
        )
    )
    assert len(project_id_1)

    project_id_2 = asyncio.run(
        get_or_create_autopilot_run_async(
            dr_endpoint,
            dr_token,
            "pytest autopilot",
            dataset,
            analyze_and_model_config=analyze_and_model_config,
            datetime_partitioning_config=datetime_partitioning_config,
            feature_settings_config=feature_settings_config,
            advanced_options_config=advanced_options_config,
            use_case=use_case,
            calendar_id=calendar_from_dataset,
        )
    )

    assert project_id_1 == project_id_2

    analyze_and_model_config_max_wait = deepcopy(analyze_and_model_config)
    analyze_and_model_config_max_wait["max_wait"] = 1234

    project_id_2_max_wait = asyncio.run(
        get_or_create_autopilot_run_async(
            dr_endpoint,
            dr_token,
            "pytest autopilot",
            dataset,
            analyze_and_model_config=analyze_and_model_config_max_wait,
            datetime_partitioning_config=datetime_partitioning_config,
            feature_settings_config=feature_settings_config,
            advanced_options_config=advanced_options_config,
            use_case=use_case,
            calendar_id=calendar_from_dataset,
        )
    )

    assert project_id_1 == project_id_2_max_wait

    advanced_options_config["seed"] = 43

    project_id_3 = asyncio.run(
        get_or_create_autopilot_run_async(
            dr_endpoint,
            dr_token,
            "pytest autopilot",
            dataset,
            analyze_and_model_config=analyze_and_model_config,
            datetime_partitioning_config=datetime_partitioning_config,
            feature_settings_config=feature_settings_config,
            advanced_options_config=advanced_options_config,
            use_case=use_case,
            calendar_id=calendar_from_dataset,
        )
    )

    assert project_id_1 != project_id_3


def test_get_or_create_partly_configured_autopilot_run(
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


def test_get_or_create_partly_configured_autopilot_run_async(
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
    project_id = asyncio.run(
        get_or_create_autopilot_run_async(
            dr_endpoint,
            dr_token,
            "pytest partial",
            dataset,
            analyze_and_model_config=analyze_and_model_config,
            datetime_partitioning_config=datetime_partitioning_config,
            advanced_options_config=advanced_options_config,
        )
    )
    assert len(project_id)


def test_get_or_create_autopilot_run_no_wait_for_completion(
    dr_endpoint,
    dr_token,
    dataset,
    analyze_and_model_config,
    datetime_partitioning_config,
    feature_settings_config,
    advanced_options_config,
    use_case,
    calendar_from_dataset,
    cleanup_projects,
):
    # Test wait_for_completion=False for sync function
    project_id = get_or_create_autopilot_run(
        dr_endpoint,
        dr_token,
        "pytest autopilot no wait",
        dataset,
        analyze_and_model_config=analyze_and_model_config,
        datetime_partitioning_config=datetime_partitioning_config,
        feature_settings_config=feature_settings_config,
        advanced_options_config=advanced_options_config,
        use_case=use_case,
        calendar_id=calendar_from_dataset,
        wait_for_completion=False,
    )
    assert len(project_id)
    
    # Verify the project exists and is in modeling stage
    project = dr.Project.get(project_id)
    assert project.stage == "modeling"

    # Test wait_for_completion=False for async function using same project
    project_id_async = asyncio.run(
        get_or_create_autopilot_run_async(
            dr_endpoint,
            dr_token,
            "pytest autopilot no wait",  # Same name to reuse project
            dataset,
            analyze_and_model_config=analyze_and_model_config,
            datetime_partitioning_config=datetime_partitioning_config,
            feature_settings_config=feature_settings_config,
            advanced_options_config=advanced_options_config,
            use_case=use_case,
            calendar_id=calendar_from_dataset,
            wait_for_completion=False,
        )
    )
    # Should return the same project ID since config is identical
    assert project_id == project_id_async
