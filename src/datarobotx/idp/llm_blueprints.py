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

from datarobotx.idp.common.hashing import get_hash

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
    """Get or create an LLM blueprint with requested parameters.

    Notes
    -----
    Saves the blueprint after creation.
    """
    dr.Client(token=token, endpoint=endpoint)  # type: ignore
    try:
        return _find_existing_llm_blueprint(
            playground=playground, name=name, is_saved=True, **kwargs
        )
    except KeyError:
        vdb_settings = kwargs.pop("vector_database_settings", None)
        if isinstance(vdb_settings, dict):
            vdb_settings = VectorDatabaseSettings(**vdb_settings)
        bp = LLMBlueprint.create(playground, name, vector_database_settings=vdb_settings, **kwargs)
        bp.update(is_saved=True)
        return str(bp.id)


def get_or_register_llm_blueprint_custom_model_version(
    endpoint: str, token: str, llm_blueprint_id: str, **kwargs: Any
) -> str:
    """Get or register a custom model version from an LLM blueprint.

    Notes
    -----
    Records a checksum in the custom model description to allow future calls to this
    function to validate whether a desired model version already exists
    """
    dr.Client(token=token, endpoint=endpoint)  # type: ignore
    bp = LLMBlueprint.get(llm_blueprint_id)
    bp_token = get_hash(
        llm_blueprint_id,
        bp.name,
        bp.description,
        bp.playground_id,
        bp.llm_id,
        bp.llm_settings,
        str(bp.prompt_type),
        bp.vector_database_id,
        bp.vector_database_settings.to_dict() if bp.vector_database_settings is not None else None,
    )
    for cm in dr.CustomInferenceModel.list(search_for=bp_token):  # type: ignore
        if cm.description is not None and bp_token in cm.description:
            if cm.latest_version is not None:
                return str(cm.latest_version.id)
    else:
        cm_version = bp.register_custom_model(**kwargs)
        cm = dr.CustomInferenceModel.get(cm_version.custom_model_id)  # type: ignore
        cm.update(description=f"Checksum: {bp_token}")
        return str(cm_version.id)
