
import pandas as pd
def load_model(code_dir):
    pass
def score(data, model, **kwargs):
    try:
        inp = {"question": data.iloc[0]["question"]}
        outputs = {"answer": f"You asked me \"{inp['question']}\""}
        rv = outputs["answer"]
    except Exception as e:
        rv = f"{e.__class__.__name__}: {str(e)}"
    return pd.DataFrame({"answer": [rv]})
