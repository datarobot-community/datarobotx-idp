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
from pathlib import Path
from typing import Union

import datarobot as dr
from datarobot import CalendarFile  # type: ignore[attr-defined]

from datarobotx.idp.common.hashing import get_hash


def _find_existing_calendar(calendar_token: str) -> str:
    for calendar in CalendarFile.list():
        if calendar_token in str(calendar.name):
            return str(calendar.id)
    raise KeyError("No matching calendar found")


def get_or_create_calendar_dataset_from_country_code(
    endpoint: str,
    token: str,
    name: str,
    country_code: str,
    start_date: Union[str, dt.datetime],
    end_date: Union[str, dt.datetime],
) -> str:
    """Create or retrieve a calendar dataset from a country code.

    Parameters
    ----------
    name : str
        Name for the calendar
    country_code : str
        The country code to use for the calendar.
    start_date : str | datetime
        The earliest date to include in the generated calendar.
        If a string, it must be in the format 'YYYY-MM-DD'.
    end_date : str | datetime
        The latest date to include in the generated calendar.
        If a string, it must be in the format 'YYYY-MM-DD'.

    Returns
    -------
    str
        The ID of the created or retrieved calendar.
    """
    dr.Client(token=token, endpoint=endpoint)  # type: ignore

    calendar_token = get_hash(name, country_code, start_date, end_date)
    try:
        return _find_existing_calendar(calendar_token=calendar_token)
    except KeyError:
        if isinstance(start_date, str):
            start_date = dt.datetime.strptime(start_date, "%Y-%m-%d")
        if isinstance(end_date, str):
            end_date = dt.datetime.strptime(end_date, "%Y-%m-%d")
        calendar_id = str(
            dr.CalendarFile.create_calendar_from_country_code(  # type: ignore[attr-defined]
                country_code, start_date=start_date, end_date=end_date
            ).id
        )
        dr.CalendarFile.update_name(calendar_id, new_calendar_name=f"{name} [{calendar_token}]")  # type: ignore[attr-defined]
        return calendar_id


def get_or_create_calendar_dataset_from_file(
    endpoint: str,
    token: str,
    name: str,
    file_path: str,
) -> str:
    """Create or retrieve a calendar dataset from a file.

    Parameters
    ----------
    name : str
        Name for the calendar
    file_path : str
        The path to the file to use for the calendar.

    Returns
    -------
    str
        The ID of the created or retrieved calendar.
    """
    dr.Client(token=token, endpoint=endpoint)  # type: ignore

    calendar_token = get_hash(name, Path(file_path))
    try:
        return _find_existing_calendar(calendar_token=calendar_token)
    except KeyError:
        calendar_id = str(dr.CalendarFile.create(file_path).id)  # type: ignore[attr-defined]
        dr.CalendarFile.update_name(calendar_id, new_calendar_name=f"{name} [{calendar_token}]")  # type: ignore[attr-defined]
        return calendar_id
