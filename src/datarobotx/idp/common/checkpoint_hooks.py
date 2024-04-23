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

import functools
import logging
from typing import Any, Dict

try:
    from kedro.framework.hooks import hook_impl
    from kedro.io import DataCatalog
    from kedro.pipeline.node import Node
except ImportError as e:
    raise ImportError("Consider including kedro in your project requirements`") from e

from datarobotx.idp.common.hashing import get_hash


class CheckpointHooks:
    """Attempt to checkpoint/cache for nodes that request it.

    Users can request a node use checkpointing by setting the tag 'checkpoint' on the node.
    """

    def __init__(self) -> None:
        self.node_original_func = {}  # type: ignore
        self.node_inputs_hash = {}  # type: ignore
        self.logger = logging.getLogger(__name__)

    @property
    def checksum_tag(self) -> str:
        """Node tag indicating checkpointing has been requested."""
        return "checkpoint"

    @staticmethod
    def checksum_catalog_name(node: Node) -> str:
        """Catalog dataset name for where checksums should be stored."""
        assert len(node.name)
        return f"{node.name}_checksum"

    @hook_impl
    def before_node_run(
        self,
        node: Node,
        catalog: DataCatalog,
        inputs: Dict[str, Any],
        is_async: bool,
        session_id: str,
    ) -> None:
        """Attempt to skip node execution if node previously completed with same inputs."""
        if self.checksum_tag not in node.tags:
            return

        checksum = ""
        try:
            checksum = get_hash(node.func, **inputs)
            checksum_catalog_name = self.checksum_catalog_name(node)
            prior_checksum = catalog.load(checksum_catalog_name)
            assert prior_checksum == checksum

            self.logger.info(f"Loading previously checkpointed outputs for node: {node.name}...")
            outputs_dict = {name: catalog.load(name) for name in node.outputs}
            if len(node.outputs) == 1:
                outputs = outputs_dict[node.outputs[0]]
            else:
                outputs = tuple(outputs_dict[name] for name in node.outputs)

            self.node_original_func[id(node)] = node.func
            node.func = functools.wraps(node.func)(lambda *args, **kwargs: outputs)
        except Exception:
            if len(checksum):
                self.node_inputs_hash[id(node)] = checksum

    @hook_impl
    def after_node_run(
        self,
        node: Node,
        catalog: DataCatalog,
        inputs: Dict[str, Any],
        outputs: Dict[str, Any],
        is_async: bool,
        session_id: str,
    ) -> None:
        """Store checksum for checkpointed nodes after execution."""
        if self.checksum_tag in node.tags:
            if id(node) in self.node_original_func:
                node.func = self.node_original_func.pop(id(node))
            if id(node) in self.node_inputs_hash:
                checksum_catalog_name = self.checksum_catalog_name(node)
                checksum = self.node_inputs_hash.pop(id(node))
                self.logger.info(f"Recording an outputs checkpoint for node: {node.name}...")
                catalog.save(checksum_catalog_name, checksum)
