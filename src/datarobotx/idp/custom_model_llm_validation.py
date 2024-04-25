from typing import Any

from datarobot.models.genai.custom_model_llm_validation import CustomModelLLMValidation


def get_or_create_custom_model_llm_validation(
    endpoint: str,
    token: str,
    prompt_column_name: str,
    target_column_name: str,
    deployment_id: str,
    **kwags: Any,
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
    kwags : Any
        Additional parameters to create the validation record.
        Valid parameters:
            model : Optional[Union[Model, str]], optional
                The specific model within the deployment, either `Model` or model ID.
                If not specified, the underlying model ID will be derived from the
                deployment info automatically.
            use_case : Optional[Union[UseCase, str]], optional
                The Use Case to link the validation to, either `UseCase` or Use Case ID.
            name : Optional[str], optional
                The name of the validation.
            wait_for_completion : bool
                If set to True code will wait for the validation job to complete before
                returning the result (up to 10 minutes, raising timeout error after that).
                Otherwise, you can check current validation status by using
                CustomModelValidation.get with returned ID.
            prediction_timeout : Optional[int], optional
                The timeout, in seconds, for the prediction API used in this custom model validation.

    Returns
    -------
    str
        ID of the validation record.
    """
    try:
        validation = CustomModelLLMValidation.list(
            deployment=deployment_id,
            prompt_column_name=prompt_column_name,
            target_column_name=target_column_name,
        )[0]
        return str(validation.id)
    except:
        pass
    validation = CustomModelLLMValidation.create(
        prompt_column_name=prompt_column_name,
        target_column_name=target_column_name,
        deployment_id=deployment_id,
        **kwags,
    )
    return str(validation.id)
