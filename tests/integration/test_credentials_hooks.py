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

from kedro.io import DataCatalog

from datarobotx.idp.common.credentials_hooks import CredentialsHooks


def test_credentials_hooks(dr_token, dr_endpoint):
    creds = {"datarobot": {"api_token": dr_token, "endpoint": dr_endpoint}}
    catalog = DataCatalog.from_config({}, creds)
    hook = CredentialsHooks()
    hook.after_catalog_created(
        catalog,
        {},
        creds,
        "",
        {},
    )
    value = catalog.load("params:credentials.datarobot.api_token")
    assert value == dr_token
