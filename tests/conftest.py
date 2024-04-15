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
