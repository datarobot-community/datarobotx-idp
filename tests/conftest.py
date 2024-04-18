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

import os

import pytest


@pytest.fixture
def dr_token():
    """DR api token."""
    return os.environ["DATAROBOT_API_TOKEN"]


@pytest.fixture
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
