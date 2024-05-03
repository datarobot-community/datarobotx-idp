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

"""Fixtures common to all tests."""

from hashlib import sha256
import os

import pytest


def pytest_addoption(parser):
    """Parse command line arguments."""
    parser.addoption(
        "--preserve_dr_assets",
        action="store",
        default=False,
        help="When set, will not delete created datarobot assets when done.",
    )


@pytest.fixture(scope="module")
def dr_token():
    """DR api token."""
    return os.environ["DATAROBOT_API_TOKEN"]


@pytest.fixture(scope="module")
def dr_token_hash(dr_token):
    """
    DR api token hash.

    Used to avoid organization pytest conflicts.
    """
    token_hash = sha256(dr_token.encode("utf-8")).hexdigest()
    return token_hash[:7]


@pytest.fixture(scope="module")
def dr_endpoint():
    """DR api endpoint."""
    return os.environ["DATAROBOT_ENDPOINT"]


@pytest.fixture()
def default_prediction_server_id():
    """DR prediction server id."""
    return "5f06612df1740600260aca72"


@pytest.fixture
def sklearn_drop_in_env():
    """Sklearn drop-in environment id."""
    return "5e8c889607389fe0f466c72d"


@pytest.fixture(scope="module")
def snowflake_user():
    """DR api token."""
    return os.environ["SNOWFLAKE_USER"]


@pytest.fixture(scope="module")
def snowflake_password():
    """DR api endpoint."""
    return os.environ["SNOWFLAKE_PASSWORD"]


@pytest.fixture(scope="module")
def snowflake_jdbc_url():
    return (
        "jdbc:snowflake://{account}.snowflakecomputing.com/?warehouse={warehouse}&db={db}".format(
            account="datarobot_partner", warehouse="DEMO_WH", db="SANDBOX"
        )
    )


@pytest.fixture(scope="module")
def snowflake_driver_id():
    return "6409af3ae3ad5839b69a4daa"


@pytest.fixture(scope="module")
def snowflake_training_table_name():
    return {"table": "LENDING_CLUB_10K", "schema": "TRAINING"}


@pytest.fixture(scope="module")
def snowflake_scoring_table_name():
    return {"table": "LENDING_CLUB_10K", "schema": "SCORING"}
