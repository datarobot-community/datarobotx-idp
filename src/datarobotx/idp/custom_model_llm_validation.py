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

import time
from typing import Any, Tuple, Union

import datarobot as dr

try:
    from datarobot.models.genai.custom_model_llm_validation import CustomModelLLMValidation
except ImportError as e:
    raise ImportError("datarobot>=3.4.0 is required for CustomModelLLMValidation support") from e


def _find_existing_validation(
    timeout_secs: int,
    deployment_id: Union[str, dr.Deployment],  # type: ignore
    **kwargs: Any,
) -> Tuple[str, str]:
    use_case = kwargs.pop("use_case", None)
    validation = CustomModelLLMValidation.list(
        deployment=deployment_id,
        use_cases=use_case,
    )[0]

    waited_secs = 0
    while True:
        validation_status = CustomModelLLMValidation.get(validation.id).validation_status
        if validation_status == "PASSED" and all(
            getattr(validation, key) == kwargs[key] for key in kwargs
        ):
            return str(validation.id), "GET"
        elif validation_status in ["FAILED", "PASSED"]:
            return str(validation.id), "PATCH"
        elif waited_secs > timeout_secs:
            raise TimeoutError("Timed out waiting for LLM validation to finish validating.")
        time.sleep(3)
        waited_secs += 3


def get_update_or_create_custom_model_llm_validation(
    endpoint: str,
    token: str,
    prompt_column_name: str,
    target_column_name: str,
    deployment_id: Union[str, dr.Deployment],  # type: ignore
    **kwargs: Any,
) -> str:
    """
    Get or create a custom model LLM validation record.

    If a validation record with the requested parameters already exists, return its ID.
    If no validation record with the requested parameters exists, it will be created.

    Parameters
    ----------
    prompt_column_name : str
        The column name the deployed model expects as the input.
    target_column_name : str
        The target name that the deployed model will output.
    deployment_id : str
        ID of the deployment.

    Returns
    -------
    str
        ID of the validation record.
    """
    dr.Client(token=token, endpoint=endpoint)  # type: ignore
    name = kwargs.pop("name", None)
    if name is None:
        deployment = dr.Deployment.get(deployment_id)  # type: ignore
        name = f'{deployment.label}: "{prompt_column_name}" -> "{target_column_name}"'

    try:
        existing_id, status = _find_existing_validation(
            timeout_secs=600,
            deployment_id=deployment_id,
            prompt_column_name=prompt_column_name,
            target_column_name=target_column_name,
            name=name,
            **kwargs,
        )
        if status == "PATCH":
            validation = CustomModelLLMValidation.get(existing_id)
            validation.update(
                prompt_column_name=prompt_column_name,
                target_column_name=target_column_name,
                deployment=deployment_id,
                name=name,
                **kwargs,
            )
            try:
                validation.revalidate(validation.id)
            except dr.errors.ClientError:
                pass  # DR will only allow revalidation if strictly needed
        return existing_id
    except IndexError:
        validation = CustomModelLLMValidation.create(
            prompt_column_name=prompt_column_name,
            target_column_name=target_column_name,
            deployment_id=deployment_id,
            name=name,
            wait_for_completion=True,
            **kwargs,
        )
        return str(validation.id)
