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

import uuid

from kedro.io import DataCatalog
from kedro.pipeline import node
import pytest

from datarobotx.idp.common.checkpoint_hooks import CheckpointHooks


@pytest.fixture
def counter():
    return []


@pytest.fixture
def dummy_node(counter):
    def f(dummy_input):
        counter.append("1")
        return "foo"

    return node(
        f, inputs="dummy_input", outputs="mock_output", tags=["checkpoint"], name="dummy_node"
    )


@pytest.fixture
def catalog(tmp_path):
    input_ = tmp_path / "dummy_input.txt"
    input_.write_text("foobar")
    return DataCatalog.from_config(
        {
            "mock_output": {
                "type": "kedro_datasets.text.TextDataset",
                "filepath": tmp_path / "foo.txt",
            },
            "dummy_node_checksum": {
                "type": "kedro_datasets.text.TextDataset",
                "filepath": tmp_path / "checksum.txt",
            },
            "dummy_input": {"type": "kedro_datasets.text.TextDataset", "filepath": input_},
        }
    )


def test_checkpoint_hooks(dummy_node, catalog, counter):
    hooks = CheckpointHooks()
    uid = str(uuid.uuid4())
    inputs = {"dummy_input": catalog.load("dummy_input")}
    hooks.before_node_run(dummy_node, catalog, inputs, False, uid)
    outputs = dummy_node.run(inputs)
    for output in outputs:
        catalog.save(output, outputs[output])
    hooks.after_node_run(dummy_node, catalog, inputs, outputs, False, uid)

    assert len(catalog.load("dummy_node_checksum"))
    assert len(counter) == 1

    hooks.before_node_run(dummy_node, catalog, inputs, False, uid)
    outputs_deux = dummy_node.run(inputs)
    hooks.after_node_run(dummy_node, catalog, inputs, outputs, False, uid)
    assert len(counter) == 1
    assert outputs_deux == outputs
