# Overview
Collection of unofficial idempotent DataRobot helpers. Ready for use in your orchestration tool of choice. 

# Installation
```bash
pip install datarobotx-idp
```

# Usage
```python
import os
from datarobotx.idp.execution_environments import get_or_create_execution_environment

dr_endpoint = os.environ['DATAROBOT_ENDPOINT']
dr_token = os.environ['DATAROBOT_API_TOKEN']

env_id_1 = get_or_create_execution_environment(dr_endpoint, dr_token, "image #1")
env_id_2 = get_or_create_execution_environment(dr_endpoint, dr_token, "image #1")
assert env_id_1 == env_id_2

env_id_3 = get_or_create_execution_environment(dr_endpoint, dr_token, "image #2")
assert env_id_1 != env_id_3
```

# Contributing
## Rules
1. Public functions must be idempotent
2. Function signatures must be type hinted (enforced by mypy)
3. Functions must have numpydoc-style docstrings (enforced by ruff)
4. Functions must either have a unit or integration test

## Principles
1. Group code so it can be easily understood and edited at the .py level
2. Minimize dependencies
3. Isolate dependencies by submodule to reduce risk of dependency conflicts
