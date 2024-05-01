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
import shutil

import pytest

from datarobotx.idp.datasets import get_or_create_dataset_from_file
from datarobotx.idp.use_cases import get_or_create_use_case
from datarobotx.idp.vector_databases import get_or_create_vector_database_from_dataset


@pytest.fixture
def use_case(dr_endpoint, dr_token, cleanup_dr, debug_override):
    with cleanup_dr("useCases/", debug_override=debug_override):
        yield get_or_create_use_case(
            dr_endpoint,
            dr_token,
            "pytest use case",
        )


@pytest.fixture
def cleanup_env(cleanup_dr, debug_override):
    with cleanup_dr("genai/vectorDatabases/", debug_override=debug_override):
        yield


@pytest.fixture
def dummy_text_file(tmp_path):
    path = tmp_path / "test_txt_dir"
    path.mkdir()
    (path / "test.txt").write_text("Hello, I am a dummy text file.")
    name = shutil.make_archive(str(tmp_path / "test_archive"), "zip", root_dir=path)
    return name


@pytest.fixture
def dummy_text_dataset(
    dr_endpoint, dr_token, dummy_text_file, use_case, cleanup_dr, debug_override
):
    with cleanup_dr("datasets/", id_attribute="datasetId", debug_override=debug_override):
        yield get_or_create_dataset_from_file(
            dr_endpoint,
            dr_token,
            use_case,
            "pytest dataset #1",
            dummy_text_file,
        )


@pytest.fixture
def chunking_parameters():
    return {
        "embedding_model": "jinaai/jina-embedding-t-en-v1",
        "chunk_size": 256,
        "chunking_method": "recursive",
        "chunk_overlap_percentage": 0,
        "separators": ["\n\n", "\n", " ", ""],
    }


def test_get_or_create(
    dr_endpoint, dr_token, use_case, dummy_text_dataset, chunking_parameters, cleanup_env
):
    db_1 = get_or_create_vector_database_from_dataset(
        dr_endpoint,
        dr_token,
        dummy_text_dataset,
        chunking_parameters,
        name="pytest vdb #1",
        use_case=use_case,
    )
    assert len(db_1)

    db_2 = get_or_create_vector_database_from_dataset(
        dr_endpoint,
        dr_token,
        dummy_text_dataset,
        chunking_parameters,
        name="pytest vdb #1",
        use_case=use_case,
    )
    assert db_1 == db_2

    db_3 = get_or_create_vector_database_from_dataset(
        dr_endpoint,
        dr_token,
        dummy_text_dataset,
        chunking_parameters,
        name="pytest vdb #2",
        use_case=use_case,
    )
    assert db_1 != db_3
