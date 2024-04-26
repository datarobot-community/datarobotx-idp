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

# mypy: disable-error-code="attr-defined"
# pyright: reportPrivateImportUsage=false
from typing import Any

import datarobot as dr


def _find_existing_custom_model(**kwargs: Any) -> str:
    for model in dr.CustomInferenceModel.list():
        if all(getattr(model, key) == kwargs[key] for key in kwargs):
            return str(model.id)
    raise KeyError("No matching custom model found")


def get_or_create_custom_model(
    endpoint: str, token: str, name: str, target_type: str, **kwargs: Any
) -> str:
    """Get or create a custom model with requested parameters."""
    dr.Client(endpoint=endpoint, token=token)
    try:
        return _find_existing_custom_model(name=name, target_type=target_type, **kwargs)
    except KeyError:
        return str(dr.CustomInferenceModel.create(name=name, target_type=target_type, **kwargs).id)
