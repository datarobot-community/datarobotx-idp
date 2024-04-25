from datarobotx.idp.custom_model_llm_validation import get_or_create_custom_model_llm_validation
 

def test_get_or_create_custom_model_llm_validation(
    dr_token, dr_endpoint, deployment_id, prompt_column_name, target_column_name
):