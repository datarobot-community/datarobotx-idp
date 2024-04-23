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

from typing import Any, Dict

try:
    from kedro.framework.hooks import hook_impl
    from kedro.io import DataCatalog
except ImportError as e:
    raise ImportError("Consider including kedro in your project requirements`") from e


class AnalyticsHooks:
    """Report recipe usage analytics to DR.

    Comment out this hook in your kedro project settings.py if you do not
    wish analytics to be reported. No customer code or datasets are included
    in the analytics captured.
    """

    def __init__(self, analytics_trace_id: str):
        self.analytics_trace_id = analytics_trace_id

    @hook_impl
    def after_catalog_created(
        self,
        catalog: DataCatalog,
        conf_catalog: Dict[str, Any],
        conf_creds: Dict[str, Any],
        save_version: str,
        load_versions: Dict[str, str],
    ) -> None:
        """Set the DataRobot client analytics trace upon catalog creation."""
        try:
            self.set_analytics_trace(conf_creds, self.analytics_trace_id)
        except Exception:
            pass

    @staticmethod
    def set_analytics_trace(conf_creds: Dict[str, Any], analytics_trace_id: str) -> None:
        """Set DataRobot client analytics trace."""
        import uuid

        import datarobot as dr

        dr_creds = conf_creds["datarobot"]
        unique_id = uuid.uuid4()
        trace_name = f"recipe->{analytics_trace_id}->{unique_id}"

        client = dr.Client(  # type: ignore
            token=dr_creds["api_token"], endpoint=dr_creds["endpoint"], trace_context=trace_name
        )
        client.get("version/")
