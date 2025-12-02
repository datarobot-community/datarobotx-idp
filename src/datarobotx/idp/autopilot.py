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
from typing import Any, Dict, List, Optional, Tuple

import datarobot as dr

from datarobotx.idp.common.hashing import get_hash
from datarobotx.idp.projects import get_or_create_project_from_dataset_async


def _find_existing_project(project_config_token: str) -> Optional[str]:
    """Return first project where token matches."""
    try:
        project = dr.Project.list(search_params={"project_name": project_config_token})[0].id  # type: ignore[attr-defined]
        return project
    except IndexError as exc:
        raise KeyError("No matching project found") from exc


def _reconcile_config_dictionaries(
    create_from_dataset_config: Optional[Dict[str, Any]] = None,
    analyze_and_model_config: Optional[Dict[str, Any]] = None,
    datetime_partitioning_config: Optional[Dict[str, Any]] = None,
    feature_settings_config: Optional[List[Dict[str, Any]]] = None,
    advanced_options_config: Optional[Dict[str, Any]] = None,
    use_case: Optional[str] = None,
    calendar_id: Optional[str] = None,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Reconcile the configuration dictionaries and return a list of them.

    Parameters
    ----------
    create_from_dataset_config : Dict[str, Any]
        The configuration for the project upload. See dr.Project.create_from_dataset
    analyze_and_model_config : Dict[str, Any]
        The configuration for the autopilot run. See dr.Project.analyze_and_model
    datetime_partitioning_config : Dict[str, Any]
        The configuration for the timeseries run. See dr.Project.set_datetime_partitioning
    feature_settings_config : Dict[str, Any]
        The configuration for known in advance and do not derive feature settings.
        See dr.Project.set_feature_settings
    advanced_options_config : Dict[str, Any]
        The configuration for the advanced options. See dr.Project.set_options
    use_case : str, optional
        The use case to build the project under, by default None.
        If provided, the project will exist under the usecase
    calendar_id : str, optional
        The calendar id to use for the project, by default None
    """
    # Make sure the user does not modify the original dictionaries
    serverside_create_from_dataset_config = (
        {} if create_from_dataset_config is None else create_from_dataset_config.copy()
    )
    serverside_analyze_and_model_config = (
        {} if analyze_and_model_config is None else analyze_and_model_config.copy()
    )
    serverside_datetime_partitioning_config = (
        {} if datetime_partitioning_config is None else datetime_partitioning_config.copy()
    )

    if use_case is not None:
        serverside_create_from_dataset_config["use_case"] = use_case

    if datetime_partitioning_config is not None and len(datetime_partitioning_config) > 0:
        if feature_settings_config is not None:
            serverside_datetime_partitioning_config["feature_settings"] = [
                dr.FeatureSettings(**config) for config in feature_settings_config
            ]
        if calendar_id is not None:
            serverside_datetime_partitioning_config["calendar_id"] = calendar_id

        serverside_analyze_and_model_config[
            "partitioning_method"
        ] = dr.DatetimePartitioningSpecification(**serverside_datetime_partitioning_config)

    if advanced_options_config is not None:
        serverside_analyze_and_model_config["advanced_options"] = dr.AdvancedOptions(
            **advanced_options_config
        )

    return serverside_create_from_dataset_config, serverside_analyze_and_model_config


def create_segmentation_task_id(
    project_id: str,
    analyze_and_model_config: Dict[str, Any],
    user_defined_segment_id_columns: List[str],
) -> str:
    """Create a segmentation task for a project.

    Parameters
    ----------
    project_id : str
        The project id to create the segmentation task for
    analyze_and_model_config: Dict[str, Any]
        The configuration for the autopilot run.
        See dr.Project.analyze_and_model
    user_defined_segment_id_columns : List[str]
        The column to use for segmented modeling (time series only).
        Must be a list of length 1.
    """
    segmentation_task_results = dr.SegmentationTask.create(  # type: ignore[attr-defined]
        project_id=project_id,
        target=analyze_and_model_config["target"],
        use_time_series=analyze_and_model_config["partitioning_method"].use_time_series,
        datetime_partition_column=analyze_and_model_config[
            "partitioning_method"
        ].datetime_partition_column,
        multiseries_id_columns=analyze_and_model_config[
            "partitioning_method"
        ].multiseries_id_columns,
        user_defined_segment_id_columns=user_defined_segment_id_columns,
    )
    segmentation_task = segmentation_task_results["completedJobs"][0]
    return segmentation_task.id


async def _wait_for_autopilot_async(project_id: str, max_wait: int = 3600) -> None:
    """Wait for autopilot to complete using async polling.

    Parameters
    ----------
    project_id : str
        The project ID to wait for
    max_wait : int
        Maximum time in seconds to wait for autopilot to complete
    """
    check_interval = 20
    waited_secs = 0

    while waited_secs < max_wait:
        # Run Project.get in thread to avoid blocking
        project = await asyncio.to_thread(dr.Project.get, project_id)  # type: ignore[attr-defined]
        if project.stage != "modeling":
            return
        await asyncio.sleep(check_interval)
        waited_secs += check_interval

    raise TimeoutError(f"Autopilot did not complete within {max_wait} seconds")


def _wait_for_autopilot(project_id: str, max_wait: int = 3600) -> None:
    """Wait for autopilot to complete using blocking polling.

    Parameters
    ----------
    project_id : str
        The project ID to wait for
    max_wait : int
        Maximum time in seconds to wait for autopilot to complete
    """
    asyncio.run(_wait_for_autopilot_async(project_id, max_wait))


async def get_or_create_autopilot_run_async(
    endpoint: str,
    token: str,
    name: str,
    dataset_id: str,
    create_from_dataset_config: Optional[Dict[str, Any]] = None,
    analyze_and_model_config: Optional[Dict[str, Any]] = None,
    datetime_partitioning_config: Optional[Dict[str, Any]] = None,
    feature_settings_config: Optional[List[Dict[str, Any]]] = None,
    advanced_options_config: Optional[Dict[str, Any]] = None,
    use_case: Optional[str] = None,
    user_defined_segment_id_columns: Optional[List[str]] = None,
    calendar_id: Optional[str] = None,
) -> Optional[str]:
    """Get or create a new project with requested parameters (async version).

    Should include all configuration for autopilot project including target, metric, etc..

    Parameters
    ----------
    name : str
        The name of the project. A hash will be appended
    dataset_id : str
        The dataset id to use for the project
    create_from_dataset_config : Dict[str, Any]
        The configuration for the project upload. See dr.Project.create_from_dataset
    analyze_and_model_config : Dict[str, Any]
        The configuration for the autopilot run. See dr.Project.analyze_and_model
    datetime_partitioning_config : Dict[str, Any]
        The configuration for the timeseries run. See dr.Project.set_datetime_partitioning
    feature_settings_config : Dict[str, Any]
        The configuration for known in advance and do not derive feature settings.
        See dr.Project.set_feature_settings
    advanced_options_config : Dict[str, Any]
        The configuration for the advanced options. See dr.Project.set_options
    use_case : str, optional
        The use case to build the project under, by default None.
        If provided, the project will exist under the usecase
    user_defined_segment_id_columns : str, optional
        The column to use for segmented modeling (time series only), by default None
    calendar_id : str, optional
        The calendar id to use for the project, by default None
    """
    # Run dr.Client initialization in thread to avoid blocking
    await asyncio.to_thread(dr.Client, token=token, endpoint=endpoint)  # type: ignore[attr-defined]

    # pull out arguments that are not relevant for hashing
    max_wait_create_from_dataset = dr.enums.DEFAULT_MAX_WAIT
    max_wait_analyze_and_model = dr.enums.DEFAULT_MAX_WAIT
    worker_count_analyze_and_model = None

    if create_from_dataset_config is not None:
        max_wait_create_from_dataset = create_from_dataset_config.pop(
            "max_wait", dr.enums.DEFAULT_MAX_WAIT
        )

    if analyze_and_model_config is not None:
        max_wait_analyze_and_model = analyze_and_model_config.pop(
            "max_wait", dr.enums.DEFAULT_MAX_WAIT
        )
        worker_count_analyze_and_model = analyze_and_model_config.pop("worker_count", None)

    project_config_token = get_hash(
        name,
        dataset_id,
        create_from_dataset_config,
        analyze_and_model_config,
        datetime_partitioning_config,
        feature_settings_config,
        advanced_options_config,
        use_case,
        user_defined_segment_id_columns,
    )

    try:
        # Run project search and get in thread to avoid blocking
        project_id_str = await asyncio.to_thread(_find_existing_project, project_config_token)
        project = await asyncio.to_thread(dr.Project.get, str(project_id_str))  # type: ignore[attr-defined]
        if project.stage == "modeling":
            # Make sure project is done
            await _wait_for_autopilot_async(str(project.id), max_wait_analyze_and_model)
            return project.id
    except KeyError:
        pass

    create_from_dataset_config, analyze_and_model_config = _reconcile_config_dictionaries(
        create_from_dataset_config,
        analyze_and_model_config,
        datetime_partitioning_config,
        feature_settings_config,
        advanced_options_config,
        use_case,
        calendar_id,
    )

    project_name = f"{name} [{project_config_token}]"

    # Use async version of get_or_create_project_from_dataset
    project_id_str = await get_or_create_project_from_dataset_async(
        endpoint=endpoint,
        token=token,
        name=project_name,
        dataset_id=dataset_id,
        max_wait=max_wait_create_from_dataset,
        **create_from_dataset_config,
    )
    project = await asyncio.to_thread(dr.Project.get, project_id_str)  # type: ignore[attr-defined]

    if user_defined_segment_id_columns is not None:
        # Run segmentation task creation in thread to avoid blocking
        segmentation_task_id = await asyncio.to_thread(
            create_segmentation_task_id,
            str(project.id),
            analyze_and_model_config=analyze_and_model_config,
            user_defined_segment_id_columns=user_defined_segment_id_columns,
        )
        analyze_and_model_config["segmentation_task_id"] = segmentation_task_id

    # Run analyze_and_model in thread to avoid blocking the event loop
    await asyncio.to_thread(
        project.analyze_and_model,
        max_wait=max_wait_analyze_and_model,
        worker_count=worker_count_analyze_and_model,
        **analyze_and_model_config,
    )
    await _wait_for_autopilot_async(str(project.id), max_wait_analyze_and_model)
    return project.id


def get_or_create_autopilot_run(
    endpoint: str,
    token: str,
    name: str,
    dataset_id: str,
    create_from_dataset_config: Optional[Dict[str, Any]] = None,
    analyze_and_model_config: Optional[Dict[str, Any]] = None,
    datetime_partitioning_config: Optional[Dict[str, Any]] = None,
    feature_settings_config: Optional[List[Dict[str, Any]]] = None,
    advanced_options_config: Optional[Dict[str, Any]] = None,
    use_case: Optional[str] = None,
    user_defined_segment_id_columns: Optional[List[str]] = None,
    calendar_id: Optional[str] = None,
) -> Optional[str]:
    """Get or create a new project with requested parameters.

    Should include all configuration for autopilot project including target, metric, etc..

    Parameters
    ----------
    name : str
        The name of the project. A hash will be appended
    dataset_id : str
        The dataset id to use for the project
    create_from_dataset_config : Dict[str, Any]
        The configuration for the project upload. See dr.Project.create_from_dataset
    analyze_and_model_config : Dict[str, Any]
        The configuration for the autopilot run. See dr.Project.analyze_and_model
    datetime_partitioning_config : Dict[str, Any]
        The configuration for the timeseries run. See dr.Project.set_datetime_partitioning
    feature_settings_config : Dict[str, Any]
        The configuration for known in advance and do not derive feature settings.
        See dr.Project.set_feature_settings
    advanced_options_config : Dict[str, Any]
        The configuration for the advanced options. See dr.Project.set_options
    use_case : str, optional
        The use case to build the project under, by default None.
        If provided, the project will exist under the usecase
    user_defined_segment_id_columns : str, optional
        The column to use for segmented modeling (time series only), by default None
    calendar_id : str, optional
        The calendar id to use for the project, by default None
    """
    return asyncio.run(
        get_or_create_autopilot_run_async(
            endpoint,
            token,
            name,
            dataset_id,
            create_from_dataset_config,
            analyze_and_model_config,
            datetime_partitioning_config,
            feature_settings_config,
            advanced_options_config,
            use_case,
            user_defined_segment_id_columns,
            calendar_id,
        )
    )
