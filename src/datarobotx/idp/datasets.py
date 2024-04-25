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

from pathlib import Path
import time
from typing import Any, Optional

import pandas as pd

import datarobot as dr
from datarobot import Dataset  # type: ignore
from datarobot.models.use_cases.utils import UseCaseLike

from datarobotx.idp.common.hashing import get_hash


def _find_existing_dataset(
    timeout_secs: int, dataset_token: str, use_cases: Optional[UseCaseLike] = None
) -> str:
    for dataset in Dataset.list(use_cases=use_cases):
        if dataset_token in dataset.name:
            waited_secs = 0
            while True:
                status = Dataset.get(dataset.id).processing_state
                if status == "COMPLETED":
                    return str(dataset.id)
                elif status == "ERROR":
                    break
                elif waited_secs > timeout_secs:
                    raise TimeoutError("Timed out waiting for dataset to process.")
                time.sleep(3)
                waited_secs += 3

    raise KeyError("No matching dataset found")


def get_or_create_dataset_from_file(
    endpoint: str,
    token: str,
    name: str,
    file_path: str,
    use_cases: Optional[UseCaseLike] = None,
    **kwargs: Any,
) -> str:
    """Get or create a DR dataset from a file with requested parameters.

    Notes
    -----
    Records a checksum in the dataset name to allow future calls to this
    function to validate whether a desired dataset already exists
    """
    dr.Client(token=token, endpoint=endpoint)  # type: ignore
    dataset_token = get_hash(name, Path(file_path), use_cases, **kwargs)

    try:
        return _find_existing_dataset(
            timeout_secs=600, dataset_token=dataset_token, use_cases=use_cases
        )
    except KeyError:
        dataset: Dataset = Dataset.create_from_file(file_path=file_path, use_cases=use_cases)
        # Dataset API does not have a description attribute (also not exposed in Workbench UI)
        dataset.modify(name=f"{name} [{dataset_token}]")
        return str(dataset.id)


def get_or_create_dataset_from_df(
    endpoint: str,
    token: str,
    name: str,
    data_frame: pd.DataFrame,
    use_cases: Optional[UseCaseLike] = None,
    **kwargs: Any,
) -> str:
    """Get or create a DR dataset from a dataframe with requested parameters.

    Notes
    -----
    Records a checksum in the dataset name to allow future calls to this
    function to validate whether a desired dataset already exists
    """
    dr.Client(token=token, endpoint=endpoint)  # type: ignore
    dataset_token = get_hash(name, data_frame, use_cases, **kwargs)

    try:
        return _find_existing_dataset(
            timeout_secs=600, dataset_token=dataset_token, use_cases=use_cases
        )
    except KeyError:
        dataset: Dataset = Dataset.create_from_in_memory_data(
            data_frame=data_frame, use_cases=use_cases
        )
        # Dataset API does not have a description attribute (also not exposed in Workbench UI)
        dataset.modify(name=f"{name} [{dataset_token}]")
        return str(dataset.id)


def get_or_create_dataset_from_datasource(
    endpoint: str,
    token: str,
    name: str,
    data_source_id: str,
    use_cases: Optional[UseCaseLike] = None,
    **kwargs: Any,
) -> str:
    """
    Get or create a DR dataset from a datasource with requested parameters.

    Notes
    -----
    Records a checksum in the dataset name to allow future calls to this
    function to validate whether a desired dataset already exists
    """
    dr.Client(token=token, endpoint=endpoint)  # type: ignore
    dataset_token = get_hash(name, data_source_id, use_cases, **kwargs)

    try:
        return _find_existing_dataset(
            timeout_secs=600, dataset_token=dataset_token, use_cases=use_cases
        )
    except KeyError:
        pass

    dataset = Dataset.create_from_data_source(  # type: ignore
        data_source_id=data_source_id,
        use_cases=use_cases,
        **kwargs,
    )
    dataset.modify(name=f"{name} [{dataset_token}]")

    return str(dataset.id)
