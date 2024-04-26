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


from typing import Any, Tuple, Union

import datarobot as dr

from datarobotx.idp.common.hashing import get_hash

try:
    from datarobot.models.genai import Playground
    from datarobot.models.genai.llm_blueprint import (
        LLMBlueprint,
        VectorDatabaseSettings,
    )
except ImportError as e:
    raise ImportError("datarobot>=3.4.0 is required for LLMBlueprint support") from e


def _find_existing_llm_blueprint(playground: str, bp_token: str) -> str:
    for bp in LLMBlueprint.list(playground=playground):
        if bp.name is not None and bp_token in bp.name:
            return str(bp.id)
    raise KeyError("No matching LLM Blueprint found")


def get_or_create_llm_blueprint(
    endpoint: str,
    token: str,
    playground: Union[str, Playground],
    name: str,
    **kwargs: Any,
) -> str:
    """Get or create an LLM blueprint with requested parameters.

    Notes
    -----
    Saves the blueprint after creation.

    Parameters
    ----------
    endpoint : str
        The DataRobot endpoint to connect to.
    token : str
        The DataRobot API token to use.
    playground : Union[str, Playground]
        The playground to create the blueprint in.
    name : str
        The name of the blueprint.
    **kwargs : Any
        Additional keyword arguments to pass to the blueprint creation.
        check datarobot.models.genai.llm_blueprint.LLMBlueprint.create for more details.

    Returns
    -------
    str
        The ID of the LLM blueprint.
    """
    dr.Client(token=token, endpoint=endpoint)  # type: ignore

    if isinstance(playground, Playground):
        playground = str(Playground.id)
    vdb_settings = kwargs.pop("vector_database_settings", None)
    if isinstance(vdb_settings, dict):
        vdb_settings = VectorDatabaseSettings(**vdb_settings)
    bp_token = get_hash(playground, name, **kwargs)

    try:
        return _find_existing_llm_blueprint(playground, bp_token)
    except KeyError:
        name = f"{name} [{bp_token}]"
        bp = LLMBlueprint.create(playground, name, vector_database_settings=vdb_settings, **kwargs)
        bp.update(is_saved=True)
        return str(bp.id)


def get_or_register_llm_blueprint_custom_model_version(
    endpoint: str, token: str, llm_blueprint_id: str, **kwargs: Any
) -> Tuple[str, str]:
    """Get or register a custom model version from an LLM blueprint.

    Notes
    -----
    Records a checksum in the custom model description to allow future calls to this
    function to validate whether a desired model version already exists

    Parameters
    ----------
    endpoint : str
        The DataRobot endpoint to connect to.
    token : str
        The DataRobot API token to use.
    llm_blueprint_id : str
        The ID of the LLM blueprint to use.
    **kwargs : Any
        Additional keyword arguments to pass to the custom model registration.
        check datarobot.models.genai.llm_blueprint.LLMBlueprint.register_custom_model for more details.

    Returns
    -------
    Tuple[str, str]
        The ID of the custom model and the ID of the custom model version.
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
        (
            bp.vector_database_settings.to_dict()
            if bp.vector_database_settings is not None
            else None
        ),
    )
    for cm in dr.CustomInferenceModel.list(search_for=bp_token):  # type: ignore
        if cm.description is not None and bp_token in cm.description:
            if cm.latest_version is not None:
                return str(cm.id), str(cm.latest_version.id)
    else:
        cm_version = bp.register_custom_model(**kwargs)
        cm = dr.CustomInferenceModel.get(cm_version.custom_model_id)  # type: ignore
        cm.update(description=f"Checksum: {bp_token}")
        return str(cm.id), str(cm_version.id)
