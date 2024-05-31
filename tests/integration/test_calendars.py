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

import datetime as dt

import pandas as pd
import pytest

from datarobotx.idp.calendars import (
    get_or_create_calendar_dataset_from_country_code,
    get_or_create_calendar_dataset_from_file,
)


@pytest.fixture
def cleanup_env(cleanup_dr):
    with cleanup_dr("calendars/", id_attribute="Id"):
        yield


@pytest.fixture
def dummy_calendar_file(tmp_path):
    dummy_calendar = pd.DataFrame(
        {
            "Date": ["2024-01-01", "2024-12-25"],
            "Name": ["New Year's Day", "Christmas Day"],
        },
    )
    path = tmp_path / "py_test.csv"
    dummy_calendar.to_csv(path, index=False)
    return str(path)


@pytest.fixture
def dummy_generated_calendar():
    return {
        "country_code": "US",
        "start_date": dt.date(2024, 1, 1),
        "end_date": dt.date(2024, 12, 31),
    }


def test_get_or_create_from_file(dr_endpoint, dr_token, dummy_calendar_file, cleanup_dr):
    ds_1 = get_or_create_calendar_dataset_from_file(
        dr_endpoint,
        dr_token,
        "pytest calendar #1",
        dummy_calendar_file,
    )
    assert len(ds_1)

    ds_2 = get_or_create_calendar_dataset_from_file(
        dr_endpoint,
        dr_token,
        "pytest calendar #1",
        dummy_calendar_file,
    )
    assert ds_1 == ds_2

    ds_3 = get_or_create_calendar_dataset_from_file(
        dr_endpoint,
        dr_token,
        "pytest calendar #2",
        dummy_calendar_file,
    )
    assert ds_1 != ds_3


def test_get_or_create_from_country_code(
    dr_endpoint, dr_token, dummy_generated_calendar, cleanup_dr
):
    ds_1 = get_or_create_calendar_dataset_from_country_code(
        dr_endpoint, dr_token, "pytest calendar #1", **dummy_generated_calendar
    )
    assert len(ds_1)

    ds_2 = get_or_create_calendar_dataset_from_country_code(
        dr_endpoint, dr_token, "pytest calendar #1", **dummy_generated_calendar
    )
    assert ds_1 == ds_2

    ds_3 = get_or_create_calendar_dataset_from_country_code(
        dr_endpoint, dr_token, "pytest calendar #2", **dummy_generated_calendar
    )
    assert ds_1 != ds_3
