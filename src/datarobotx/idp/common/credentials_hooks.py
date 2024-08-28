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

import logging
from typing import Any, Dict

try:
    from kedro.framework.hooks import hook_impl
    from kedro.io import DataCatalog
except ImportError as e:
    raise ImportError("Consider including kedro in your project requirements`") from e

from datarobotx.idp.common.handle_io import get_feed_dict


class CredentialsHooks:
    """Include credentials in the catalog so they can be used by nodes."""

    @property
    def _logger(self):
        return logging.getLogger(__name__)

    @hook_impl
    def after_catalog_created(
        self,
        catalog: DataCatalog,
        conf_creds: Dict[str, Any],
    ) -> None:
        """Validate credentials and add them to the catalog for node consumption."""
        self._validate_or_replace_credentials(conf_creds)

        creds = get_feed_dict(
            conf_creds, copy_dict_as=None, key_prefix="params:credentials."
        )
        catalog.add_feed_dict(creds)

    def _validate_or_replace_credentials(self, conf_creds: Dict[str, Any]) -> None:
        """Confirm credentials appear to be valid and functional."""
        import datarobot as dr

        dr_creds = conf_creds["datarobot"]
        try:
            dr.Client(token=dr_creds["api_token"], endpoint=dr_creds["endpoint"])  # type: ignore
        except ValueError:
            self._logger.warning(
                "Could not authenticate with DataRobot. "
                "Kedro will run dr.Client() for authentication."
            )
            client = dr.Client()
            conf_creds["datarobot"]["api_token"] = client.token
            conf_creds["datarobot"]["endpoint"] = client.endpoint
            prediction_server_id = dr.PredictionServer.list()[0].id

            if "prediction_environment_id" in dr_creds:
                conf_creds["datarobot"][
                    "prediction_environment_id"
                ] = prediction_server_id
            elif "default_prediction_server_id" in dr_creds:
                conf_creds["datarobot"][
                    "default_prediction_server_id"
                ] = prediction_server_id
