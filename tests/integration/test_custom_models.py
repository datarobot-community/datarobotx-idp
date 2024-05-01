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

from datarobotx.idp.custom_models import get_or_create_custom_model


@pytest.fixture
def cleanup_env(cleanup_dr, debug_override):
    with cleanup_dr("customModels/", debug_override=debug_override):
        yield


def test_get_or_create(dr_endpoint, dr_token, cleanup_env):
    model_id_1 = get_or_create_custom_model(
        dr_endpoint, dr_token, "pytest model #1", "Binary", target_name="foo"
    )
    assert len(model_id_1)

    model_id_2 = get_or_create_custom_model(
        dr_endpoint, dr_token, "pytest model #1", "Binary", target_name="foo"
    )
    assert model_id_1 == model_id_2

    with pytest.raises(ValueError):
        model_id_3 = get_or_create_custom_model(
            dr_endpoint, dr_token, "pytest model #2", "TextGeneration"
        )

    model_id_3 = get_or_create_custom_model(
        dr_endpoint, dr_token, "pytest model #2", "TextGeneration", target_name="bar"
    )
    assert model_id_1 != model_id_3
