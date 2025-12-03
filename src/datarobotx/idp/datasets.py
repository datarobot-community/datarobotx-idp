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

import asyncio
from pathlib import Path
from typing import Any, Optional

import pandas as pd

import datarobot as dr
from datarobot import Dataset  # type: ignore
from datarobot.models.use_cases.utils import UseCaseLike

from datarobotx.idp.common.hashing import get_hash


async def _find_existing_dataset_async(
    timeout_secs: int, dataset_token: str, use_cases: Optional[UseCaseLike] = None
) -> str:
    # Run Dataset.list in thread to avoid blocking
    datasets = await asyncio.to_thread(Dataset.list, use_cases=use_cases)
    for dataset in datasets:
        if dataset_token in dataset.name:
            waited_secs = 0
            while True:
                # Run Dataset.get in thread to avoid blocking
                status = await asyncio.to_thread(lambda: Dataset.get(dataset.id).processing_state)
                if status == "COMPLETED":
                    return str(dataset.id)
                elif status == "ERROR":
                    break
                elif waited_secs > timeout_secs:
                    raise TimeoutError("Timed out waiting for dataset to process.")
                await asyncio.sleep(3)
                waited_secs += 3

    raise KeyError("No matching dataset found")


def _find_existing_dataset(
    timeout_secs: int, dataset_token: str, use_cases: Optional[UseCaseLike] = None
) -> str:
    return asyncio.run(_find_existing_dataset_async(timeout_secs, dataset_token, use_cases))


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


async def get_or_create_dataset_from_df_async(
    endpoint: str,
    token: str,
    name: str,
    data_frame: pd.DataFrame,
    use_cases: Optional[UseCaseLike] = None,
    **kwargs: Any,
) -> str:
    """Get or create a DR dataset from a dataframe with requested parameters (async version).

    Notes
    -----
    Records a checksum in the dataset name to allow future calls to this
    function to validate whether a desired dataset already exists
    """
    # Run dr.Client initialization in thread to avoid blocking
    await asyncio.to_thread(dr.Client, token=token, endpoint=endpoint)  # type: ignore[attr-defined]
    dataset_token = get_hash(name, data_frame, use_cases, **kwargs)

    try:
        return await _find_existing_dataset_async(
            timeout_secs=600, dataset_token=dataset_token, use_cases=use_cases
        )
    except KeyError:
        # Run dataset creation in thread to avoid blocking
        dataset: Dataset = await asyncio.to_thread(
            Dataset.create_from_in_memory_data, data_frame=data_frame, use_cases=use_cases
        )
        # Dataset API does not have a description attribute (also not exposed in Workbench UI)
        # Run modify in thread to avoid blocking
        await asyncio.to_thread(dataset.modify, name=f"{name} [{dataset_token}]")
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
    return asyncio.run(
        get_or_create_dataset_from_df_async(endpoint, token, name, data_frame, use_cases, **kwargs)
    )


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
