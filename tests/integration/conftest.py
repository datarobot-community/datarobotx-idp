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

from contextlib import contextmanager
import posixpath

import pytest
import requests

import datarobot as dr
from datarobot.utils.pagination import unpaginate


@pytest.fixture(scope="session")
def global_preserve_dr_assets(pytestconfig):
    return pytestconfig.getoption("preserve_dr_assets")


@pytest.fixture(scope="module")
def cleanup_dr(dr_endpoint, dr_token, global_preserve_dr_assets):
    """Build a context manager for cleaning up DR assets."""
    headers = {"Authorization": f"Bearer {dr_token}"}

    def _make_pagination(initial_url, client):
        """Iterate over an unpaginated endpoint."""
        offset = 0
        resp = client.get(initial_url, params={"offset": offset}).json()
        while len(resp) > 0:
            yield from resp
            offset += len(resp)
            resp = client.get(initial_url, params={"offset": offset}).json()

    def _get_assets(asset_url, id_attribute, paginated=True, params=None):
        """Retrieve DR assets at a url."""
        client = dr.Client(endpoint=dr_endpoint, token=dr_token)  # type: ignore
        result = set()
        try:
            if paginated:
                for asset in unpaginate(
                    initial_url=asset_url, initial_params=params, client=client
                ):
                    result.add(asset_url + asset[id_attribute] + "/")

            else:
                # Handle legacy case with no pagination
                for asset in _make_pagination(asset_url, client):
                    result.add(asset_url + asset[id_attribute] + "/")
        except KeyError:
            pass  # if route doesn't implement standard pagination correctly (e.g. /customApplications)

        return result

    @contextmanager
    def _cleanup(
        partial_url: str,
        id_attribute: str = "id",
        preserve_dr_assets: bool = False,
        paginated=True,
        params=None,
    ):
        url = posixpath.join(dr_endpoint, partial_url)
        if url[-1] != "/":
            url = url + "/"
        assets_before = _get_assets(url, id_attribute, paginated, params)
        yield
        assets_after = _get_assets(url, id_attribute, paginated, params)
        for url in assets_after.difference(assets_before):
            if not preserve_dr_assets and not global_preserve_dr_assets:
                requests.delete(url, headers=headers)

    return _cleanup


@pytest.fixture
def custom_py():
    return """\
def load_model(input_dir):
    return ''

def score(data, model, **kwargs):
    import pandas as pd
    preds = pd.DataFrame([42 for _ in range(data.shape[0])], columns=["Predictions"])
    return preds
"""


@pytest.fixture
def folder_path(tmp_path, custom_py):
    dir = tmp_path / "custom_model"
    dir.mkdir()
    p = dir / "custom.py"
    p.write_text(custom_py)
    return str(dir.resolve())


@pytest.fixture
def another_folder_path(tmp_path, custom_py):
    dir = tmp_path / "another_custom_model"
    dir.mkdir()
    p = dir / "custom.py"
    p.write_text(custom_py)
    return str(dir.resolve())
