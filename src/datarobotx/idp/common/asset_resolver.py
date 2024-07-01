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

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Optional

if TYPE_CHECKING:
    import pathlib
    import tempfile


def prepare_yaml_content(*args: Any, **kwargs: Any) -> bytes:
    """Passthrough node for gathering content to be serialized to yaml from upstream node(s).

    Parameters
    ----------
    args : Any
        Positional arguments to be passed as a list to the yaml renderer. If
        specified, all keyword arguments will be ignored.
    kwargs : Any
        Keyword arguments to be passed as a dict to the yaml render. Ignored
        if any positional arguments are passed.
    """
    import yaml

    data: Any
    if len(args):
        data = list(args)
    else:
        data = kwargs

    return yaml.dump(data).encode("utf-8")


def render_jinja_template(
    template_file: str,
    context: Optional[Dict[str, Any]] = None,
    **additional_context: Any,
) -> bytes:
    """Render a jinja template with provided keyword arguments.

    Parameters
    ----------
    template_file : str
        Path to the jinja template to be rendered
    context : Dict[str, Any] or None
        Base keyword arguments to be passed to the jinja render() call
    additional_context : Any
        Additional keyword arguments to be passed to the jinja render() call,
        these take precedence over those specified in context

    Returns
    -------
    str :
        The template rendered with the provided keyword arguments
    """
    from jinja2 import BaseLoader, Environment

    template = Environment(loader=BaseLoader).from_string(template_file)  # type: ignore
    d = {}
    if context is None:
        context = {}
    d.update(context)
    d.update(**additional_context)

    return template.render(d).encode("utf-8")


def merge_asset_paths(
    *paths: pathlib.Path,
) -> tempfile.TemporaryDirectory[Any]:
    """Merge paths into a new temporary directory.

    Useful for grouping multiple paths into a single directory for upload to
    DataRobot. If files conflict, last specified path takes precedence.

    Parameters
    ----------
    *paths : Path
        Paths to be merged into a new temp directory

    Returns
    -------
    tempfile.TemporaryDirectory :
        Temp directory containing the merged paths.
    """
    from pathlib import Path
    import shutil
    import tempfile

    d = tempfile.TemporaryDirectory()

    temp_path = Path(d.name)

    for path in paths:
        if path.is_file():
            shutil.copy(path, temp_path)
        elif path.is_dir():
            shutil.copytree(path, temp_path, dirs_exist_ok=True)

    return d
