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
import pytest

from datarobotx.idp.common.analytics_hooks import AnalyticsHooks


@pytest.fixture
def conf_creds(dr_endpoint, dr_token):
    return {"datarobot": {"endpoint": dr_endpoint, "api_token": dr_token}}


def test_analytics_hooks(conf_creds):
    h = AnalyticsHooks("pytest")
    h.set_analytics_trace(conf_creds, "pytest")
