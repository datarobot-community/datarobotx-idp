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
    from datarobot.models.genai.llm import LLMDefinition
    from datarobot.models.genai.llm_blueprint import LLMBlueprint, VectorDatabaseSettings
except ImportError as e:
    raise ImportError("datarobot>=3.4.0 is required for LLMBlueprint support") from e


def _find_existing_llm_blueprint(**kwargs: Any) -> str:
    pg = kwargs.pop("playground", None)
    llm = kwargs.pop("llm", None)
    if isinstance(llm, str):
        kwargs["llm_id"] = llm
    elif isinstance(llm, LLMDefinition):
        kwargs["llm_id"] = llm.id
    for bp in LLMBlueprint.list(playground=pg):
        if all(getattr(bp, key) == kwargs[key] for key in kwargs):
            return str(bp.id)
    raise KeyError("No matching LLM Blueprint found")


def get_or_create_llm_blueprint(
    endpoint: str, token: str, playground: str, name: str, **kwargs: Any
) -> str:
    """Get or create an LLM blueprint with requested parameters."""
    dr.Client(token=token, endpoint=endpoint)  # type: ignore
    try:
        return _find_existing_llm_blueprint(playground=playground, name=name, **kwargs)
    except KeyError:
        if "vector_database_settings" in kwargs:
            vdb_settings = kwargs.pop("vector_database_settings")
            if isinstance(vdb_settings, dict):
                vdb_settings = VectorDatabaseSettings(**vdb_settings)
        else:
            vdb_settings = None
        bp = LLMBlueprint.create(playground, name, vector_database_settings=vdb_settings, **kwargs)
        return str(bp.id)
