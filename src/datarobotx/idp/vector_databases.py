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
from typing import Any, Dict, Union

import datarobot as dr

try:
    from datarobot.models.genai.vector_database import ChunkingParameters, VectorDatabase
except ImportError as e:
    raise ImportError("datarobot>=3.4.0 is required for VectorDatabase support") from e


def _wait_for_creation(db: VectorDatabase, timeout_secs: int) -> VectorDatabase:
    waited_secs = 0
    while True:
        db = VectorDatabase.get(db.id)
        if db.execution_status == dr.enums.VectorDatabaseExecutionStatus.COMPLETED:
            return db
        elif db.execution_status == dr.enums.VectorDatabaseExecutionStatus.ERROR:
            raise ValueError(f"VectorDatabase creation failed: {db.error_message}")
        elif waited_secs > timeout_secs:
            raise TimeoutError("Timed out waiting for VectorDatabase to build.")
        time.sleep(3)
        waited_secs += 3


def _find_existing_vector_database(timeout_secs: int, **kwargs: Any) -> str:
    use_case = kwargs.pop("use_case", None)
    kwargs["separators"] = set(kwargs["separators"])
    for db in VectorDatabase.list(use_case=use_case):
        setattr(db, "separators", set(db.separators))
        if all(getattr(db, key) == kwargs[key] for key in kwargs):
            db = _wait_for_creation(db, timeout_secs)
            return str(db.id)

    raise KeyError("No matching vector database found")


def get_or_create_vector_database_from_dataset(
    endpoint: str,
    token: str,
    dataset_id: str,
    chunking_parameters: Union[ChunkingParameters, Dict[str, Any]],
    **kwargs: Any,
) -> str:
    """Get or create a custom model with requested parameters."""
    dr.Client(endpoint=endpoint, token=token)  # type: ignore

    timeout_secs = kwargs.pop("timeout_secs", 600)
    if isinstance(chunking_parameters, ChunkingParameters):
        chunking_parameters_dict = {
            k: v
            for k, v in chunking_parameters.__dict__.items()
            if not k.startswith("_") and not callable(v)
        }
        chunking_parameters_obj = chunking_parameters
    else:
        chunking_parameters_dict = chunking_parameters
        chunking_parameters_obj = ChunkingParameters(**chunking_parameters)

    try:
        params = {"dataset_id": dataset_id}
        params.update(chunking_parameters_dict)
        params.update(kwargs)
        return _find_existing_vector_database(timeout_secs, **params)
    except KeyError:
        db = VectorDatabase.create(
            dataset_id=dataset_id, chunking_parameters=chunking_parameters_obj, **kwargs
        )
        db = _wait_for_creation(db, timeout_secs)
        return str(db.id)
