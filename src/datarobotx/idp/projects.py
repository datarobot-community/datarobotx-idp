#
# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# DataRobot, Inc.
#
# This is proprietary source code of DataRobot, Inc. and its
# affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
# mypy: disable-error-code="attr-defined"
# pyright: reportPrivateImportUsage=false
from typing import Any

import datarobot as dr


def _find_existing_project(project_name: str, dataset_id: str, dataset_version_id: str) -> str:
    for project in dr.Project.list(search_params={"project_name": project_name}):
        if project.catalog_id == dataset_id and project.catalog_version_id == dataset_version_id:
            return str(project.id)
    raise KeyError("No matching project found")


def get_or_create_project_from_dataset(
    endpoint: str, token: str, name: str, dataset_id: str, **kwargs: Any
) -> str:
    """Get or create a new project with requested parameters."""
    dr.Client(token=token, endpoint=endpoint)
    try:
        if "dataset_version_id" not in kwargs:
            kwargs["dataset_version_id"] = dr.Dataset.get(dataset_id).version_id
        return _find_existing_project(
            project_name=name,
            dataset_id=dataset_id,
            dataset_version_id=kwargs["dataset_version_id"],
        )
    except KeyError:
        project = dr.Project.create_from_dataset(dataset_id=dataset_id, project_name=name, **kwargs)
        return str(project.id)
