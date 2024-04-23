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

from typing import Any

import datarobot as dr

try:
    from datarobot.models.genai.playground import Playground
except ImportError as e:
    raise ImportError("datarobot>=3.4.0 is required for Playground support") from e


def _find_existing_playground(**kwargs: Any) -> str:
    use_case = kwargs.pop("use_case", None)
    for pg in Playground.list(use_case=use_case):
        if all(getattr(pg, key) == kwargs[key] for key in kwargs):
            return str(pg.id)
    raise KeyError("No matching playground found")


def get_or_create_playground(endpoint: str, token: str, name: str, **kwargs: Any) -> str:
    """Get or create a playground with requested parameters."""
    dr.Client(token=token, endpoint=endpoint)  # type: ignore
    try:
        return _find_existing_playground(name=name, **kwargs)
    except KeyError:
        pg = Playground.create(name, **kwargs)
        return str(pg.id)
