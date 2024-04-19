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
from typing import Any, Optional

import datarobot as dr


def _find_existing_use_case(**kwargs: Any) -> str:
    for use_case in dr.UseCase.list():
        if all([getattr(use_case, key) == kwargs[key] for key in kwargs]):
            return str(use_case.id)
    raise KeyError("No matching use case found")


def get_or_create_use_case(
    endpoint: str,
    token: str,
    name: str,
    description: Optional[str] = None,
) -> str:
    """Get or create a use case with requested parameters."""
    dr.Client(token=token, endpoint=endpoint)
    try:
        return _find_existing_use_case(name=name, description=description)
    except KeyError:
        use_case = dr.UseCase.create(
            name=name,
            description=description,
        )
        return str(use_case.id)
