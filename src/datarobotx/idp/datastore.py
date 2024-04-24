# mypy: disable-error-code="attr-defined"
# pyright: reportPrivateImportUsage=false

from typing import Any

from .common.hashing import get_hash

import datarobot as dr


def _find_existing_datastore(canonical_name: str) -> str:
    dss = dr.DataStore.list()
    datastore = [ds for ds in dss if ds.canonical_name and ds.canonical_name == canonical_name][0]
    return str(datastore.id)


def get_or_create_datastore(
    endpoint: str,
    token: str,
    canonical_name: str,
    driver_id: str,
    jdbc_url: str,
    data_store_type: str = "jdbc",
    **kwargs: Any,
) -> str:
    """Get or create a DR datastore with requested parameters.

    Notes
    -----
    Records a checksum in the datasource name to allow future calls to this
    function to validate whether a desired datasource already exists
    """
    dr.Client(token=token, endpoint=endpoint)

    datastore_token = get_hash(canonical_name, driver_id, jdbc_url, data_store_type, **kwargs)

    canonical_name = f"{canonical_name} [{datastore_token}]"
    try:
        datastore = dr.DataStore.get(_find_existing_datastore(canonical_name))
        return str(datastore.id)
    except IndexError:
        pass

    datastore = dr.DataStore.create(
        data_store_type=data_store_type,
        canonical_name=canonical_name,
        driver_id=driver_id,
        jdbc_url=jdbc_url,
        **kwargs,
    )
    return str(datastore.id)
