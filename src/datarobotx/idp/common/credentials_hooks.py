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
from typing import Any, Dict

from kedro.framework.hooks import hook_impl
from kedro.io import DataCatalog

from datarobotx.idp.common.handle_io import get_feed_dict


class CredentialsHooks:
    """Include credentials in the catalog so they can be used by nodes."""

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
        self.validate_credentials(conf_creds)

        creds = get_feed_dict(conf_creds, copy_dict_as=None, key_prefix="params:credentials.")
        catalog.add_feed_dict(creds)

    @staticmethod
    def validate_credentials(conf_creds: Dict[str, Any]):
        """Confirm credentials appear to be valid and functional."""
        import datarobot as dr

        dr_creds = conf_creds["datarobot"]

        client = dr.Client(token=dr_creds["api_token"], endpoint=dr_creds["endpoint"])
        client.get("version/")
