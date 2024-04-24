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

# mypy: disable-error-code="attr-defined"
# pyright: reportPrivateImportUsage=false
from pathlib import Path
import time
from typing import Any

import pandas as pd

import datarobot as dr
from datarobot import Dataset

from datarobotx.idp.common.hashing import get_hash


def _find_existing_dataset(timeout_secs: int, use_case_id: str, dataset_token: str) -> str:
    for dataset in dr.Dataset.list(use_cases=use_case_id):
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
    use_case_id: str,
    name: str,
    file_path: str,
    **kwargs: Any,
) -> str:
    """Get or create a DR dataset from a file with requested parameters.

    Notes
    -----
    Records a checksum in the dataset name to allow future calls to this
    function to validate whether a desired dataset already exists
    """
    dr.Client(token=token, endpoint=endpoint)
    dataset_token = get_hash(use_case_id, name, Path(file_path), **kwargs)

    try:
        return _find_existing_dataset(
            timeout_secs=600, use_case_id=use_case_id, dataset_token=dataset_token
        )
    except KeyError:
        dataset: Dataset = dr.Dataset.create_from_file(file_path=file_path, use_cases=use_case_id)
        # Dataset API does not have a description attribute (also not exposed in Workbench UI)
        dataset.modify(name=f"{name} [{dataset_token}]")
        return str(dataset.id)


def get_or_create_dataset_from_df(
    endpoint: str,
    token: str,
    use_case_id: str,
    name: str,
    data_frame: pd.DataFrame,
    **kwargs: Any,
) -> str:
    """Get or create a DR dataset from a dataframe with requested parameters.

    Notes
    -----
    Records a checksum in the dataset name to allow future calls to this
    function to validate whether a desired dataset already exists
    """
    dr.Client(token=token, endpoint=endpoint)
    dataset_token = get_hash(use_case_id, name, data_frame, **kwargs)

    try:
        return _find_existing_dataset(
            timeout_secs=600, use_case_id=use_case_id, dataset_token=dataset_token
        )
    except KeyError:
        dataset: Dataset = dr.Dataset.create_from_in_memory_data(
            data_frame=data_frame, use_cases=use_case_id
        )
        # Dataset API does not have a description attribute (also not exposed in Workbench UI)
        dataset.modify(name=f"{name} [{dataset_token}]")
        return str(dataset.id)


def get_or_create_dataset_from_datasource(
    endpoint: str,
    token: str,
    name: str,
    data_source_id: str,
    use_case_id: str,
    **kwargs: Any,
) -> str:
    """
    Get or create a DR dataset from a datasource with requested parameters.

    Notes
    -----
    Records a checksum in the dataset name to allow future calls to this
    function to validate whether a desired dataset already exists
    """
    dr.Client(token=token, endpoint=endpoint)
    dataset_token = get_hash(use_case_id, name, data_source_id, **kwargs)

    try:
        return _find_existing_dataset(
            timeout_secs=600, use_case_id=use_case_id, dataset_token=dataset_token
        )
    except KeyError:
        pass

    dataset = dr.Dataset.create_from_data_source(  # type: ignore
        use_cases=use_case_id,
        data_source_id=data_source_id,
        **kwargs,
    )
    dataset.modify(name=f"{name} [{dataset_token}]")

    return str(dataset.id)
