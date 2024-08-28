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
    def _logger(self) -> logging.Logger:
        return logging.getLogger(__name__)

    @hook_impl
    def after_catalog_created(
        self,
        catalog: DataCatalog,
        conf_catalog: Dict[str, Any],
        conf_creds: Dict[str, Any],
        save_version: str,
        load_versions: Dict[str, str],
    ) -> None:
        """Validate credentials and add them to the catalog for node consumption."""
        self._validate_or_replace_credentials(conf_creds)

        creds = get_feed_dict(conf_creds, copy_dict_as=None, key_prefix="params:credentials.")
        catalog.add_feed_dict(creds)

    def _validate_or_replace_credentials(self, conf_creds: Dict[str, Any]) -> None:
        """Confirm credentials appear to be valid and functional."""
        import datarobot as dr

        exception_occurred = False
        try:
            dr_creds = conf_creds["datarobot"]
            dr.Client(token=dr_creds["api_token"], endpoint=dr_creds["endpoint"])  # type: ignore
        except ValueError:  # Invalid or unpopulated credentials
            exception_occurred = True
        except KeyError:  # No credentials.yml file
            exception_occurred = True
            conf_creds["datarobot"] = {
                "prediction_environment_id": None,
            }

        finally:
            if exception_occurred:
                self._logger.warning(
                    "Could not authenticate with DataRobot using "
                    "credentials specified in `conf/local/credentials.yml`. "
                    "Trying credentials automatically retrieved by "
                    "importing datarobot and calling datarobot.Client()."
                )
                client = dr.Client()  # type: ignore
                available_prediction_environments = [
                    server.id
                    for server in dr.PredictionEnvironment.list()  # type: ignore
                ]

                prediction_environment_id = conf_creds["datarobot"].get(
                    "prediction_environment_id", ""
                )

                if prediction_environment_id not in available_prediction_environments:
                    prediction_environment_id = dr.PredictionServer.list()[0].id  # type: ignore

                conf_creds["datarobot"]["api_token"] = client.token
                conf_creds["datarobot"]["endpoint"] = client.endpoint
                conf_creds["datarobot"]["prediction_environment_id"] = prediction_environment_id
