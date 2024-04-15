# mypy: disable-error-code="attr-defined"
# pyright: reportPrivateImportUsage=false
from pathlib import Path
from typing import Any

import pandas as pd

import datarobot as dr

from datarobotx.idp.common.hashing import get_hash


def _find_existing_dataset(use_case_id: str, dataset_token: str) -> str:
    for dataset in dr.Dataset.list(use_cases=use_case_id):
        if dataset_token in dataset.name:
            return dataset.id
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
        return _find_existing_dataset(use_case_id, dataset_token)
    except KeyError:
        dataset = dr.Dataset.create_from_file(file_path=file_path, use_cases=use_case_id)
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
        return _find_existing_dataset(use_case_id, dataset_token)
    except KeyError:
        dataset = dr.Dataset.create_from_in_memory_data(
            data_frame=data_frame, use_cases=use_case_id
        )
        # Dataset API does not have a description attribute (also not exposed in Workbench UI)
        dataset.modify(name=f"{name} [{dataset_token}]")
        return str(dataset.id)
