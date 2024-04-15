import pytest

from datarobotx.idp.common.analytics_hooks import AnalyticsHooks


@pytest.fixture
def conf_creds(dr_endpoint, dr_token):
    return {"datarobot": {"endpoint": dr_endpoint, "api_token": dr_token}}


def test_analytics_hooks(conf_creds):
    h = AnalyticsHooks("pytest")
    h.set_analytics_trace(conf_creds, "pytest")
