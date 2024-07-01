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

import pytest

import datarobot as dr
from datarobotx.idp.credentials import get_replace_or_create_credential


@pytest.fixture
def cleanup_env(cleanup_dr):
    with cleanup_dr("credentials/", id_attribute="credentialId"):
        yield


def test_get_or_create(dr_endpoint, dr_token, cleanup_env):
    credential_1 = get_replace_or_create_credential(
        dr_endpoint, dr_token, "pytest credential #1", "api_token", api_token="foo"
    )
    credential_token_1 = dr.Credential.get(credential_1).description
    assert len(credential_1)

    credential_2 = get_replace_or_create_credential(
        dr_endpoint, dr_token, "pytest credential #1", "api_token", api_token="foo"
    )
    credential_token_2 = dr.Credential.get(credential_2).description
    assert credential_1 == credential_2
    assert credential_token_1 == credential_token_2

    # Replacements should update the existing credential, preserving the id but changing the parameters and description(checksum)
    credential_3 = get_replace_or_create_credential(
        dr_endpoint, dr_token, "pytest credential #1", "api_token", api_token="bar"
    )
    credential_token_3 = dr.Credential.get(credential_3).description
    assert credential_1 == credential_3
    assert credential_token_1 != credential_token_3

    credential_4 = get_replace_or_create_credential(
        dr_endpoint, dr_token, "pytest credential #2", "api_token", api_token="bar"
    )
    assert credential_1 != credential_4
    assert credential_3 != credential_4

    # Replacements with different credential_type should delete the existing credential, generating a new id
    credential_5 = get_replace_or_create_credential(
        dr_endpoint, dr_token, "pytest credential #2", "azure", azure_connection_string="bar"
    )
    assert credential_4 != credential_5
