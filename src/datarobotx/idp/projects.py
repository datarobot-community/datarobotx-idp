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
from typing import Any

import datarobot as dr
from datarobot import Project  # type: ignore[attr-defined]


def _find_existing_project(project_name: str, dataset_id: str, dataset_version_id: str) -> str:
    for project in dr.Project.list(search_params={"project_name": project_name}):  # type: ignore[attr-defined]
        if project.catalog_id == dataset_id and project.catalog_version_id == dataset_version_id:
            return str(project.id)
    raise KeyError("No matching project found")


async def get_or_create_project_from_dataset_async(
    endpoint: str, token: str, name: str, dataset_id: str, **kwargs: Any
) -> str:
    """Get or create a new project with requested parameters (async version)."""
    await asyncio.to_thread(dr.Client, token=token, endpoint=endpoint)  # type: ignore[attr-defined]
    try:
        if "dataset_version_id" not in kwargs:
            dataset = await asyncio.to_thread(dr.Dataset.get, dataset_id)  # type: ignore[attr-defined]
            kwargs["dataset_version_id"] = dataset.version_id
        return await asyncio.to_thread(
            _find_existing_project,
            project_name=name,
            dataset_id=dataset_id,
            dataset_version_id=kwargs["dataset_version_id"],
        )
    except KeyError:
        project: Project = await asyncio.to_thread(
            dr.Project.create_from_dataset,  # type: ignore[attr-defined]
            dataset_id=dataset_id,
            project_name=name,
            **kwargs,
        )
        return str(project.id)


def get_or_create_project_from_dataset(
    endpoint: str, token: str, name: str, dataset_id: str, **kwargs: Any
) -> str:
    """Get or create a new project with requested parameters."""
    dr.Client(token=token, endpoint=endpoint)  # type: ignore[attr-defined]
    try:
        if "dataset_version_id" not in kwargs:
            kwargs["dataset_version_id"] = dr.Dataset.get(dataset_id).version_id  # type: ignore[attr-defined]
        return _find_existing_project(
            project_name=name,
            dataset_id=dataset_id,
            dataset_version_id=kwargs["dataset_version_id"],
        )
    except KeyError:
        project: Project = dr.Project.create_from_dataset(  # type: ignore[attr-defined]
            dataset_id=dataset_id, project_name=name, **kwargs
        )
        return str(project.id)
