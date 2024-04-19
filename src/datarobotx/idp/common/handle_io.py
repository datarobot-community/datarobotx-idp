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

from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from datarobotx.idp.common.hashing import get_hash

try:
    from kedro.io import DataCatalog
except ImportError as e:
    raise ImportError("Consider including kedro in your project requirements`") from e


def _resolve_dict(o: Union[None, Dict[str, Any], Callable[[], Dict[str, Any]]]) -> Dict[str, Any]:
    """Convert an optional callable that retrieves a dict into a materialized dict."""
    if isinstance(o, dict):
        return o
    elif o is None:
        return {}
    else:
        return o()


def _persist_outputs(
    rv: Any, outputs: Union[None, str, List[str], Dict[str, str]], catalog_obj: DataCatalog
) -> None:
    """Persist wrapped-function outputs."""
    if isinstance(outputs, str):
        catalog_obj.save(outputs, rv)
    elif isinstance(outputs, list):
        for name, data in zip(outputs, rv):
            catalog_obj.save(name, data)
    elif isinstance(outputs, dict):
        for name in outputs.keys():
            catalog_obj.save(outputs[name], rv[name])


def _build_inputs(
    inputs: Union[None, str, List[str], Dict[str, str]], catalog_obj: DataCatalog
) -> Tuple[List[Any], Dict[str, Any]]:
    """Build wrapped-function calling arguments."""
    if isinstance(inputs, str):
        return [
            catalog_obj.load(inputs),
        ], {}
    elif isinstance(inputs, list):
        return [catalog_obj.load(input) for input in inputs], {}
    elif isinstance(inputs, dict):
        return [], {k: catalog_obj.load(v) for k, v in inputs.items()}
    elif inputs is None:
        return [], {}


def get_feed_dict(
    d: Dict[str, Any], copy_dict_as: Optional[str], key_prefix: str = ""
) -> Dict[str, Any]:
    """Build kedro-style feed dict to add entries to the catalog.

    e.g. parameters, credentials

    Enables access to literals using dot notation
    e.g. credentials:datarobot.endpoint
    """
    if copy_dict_as is not None:
        feed_dict = {copy_dict_as: d}
    else:
        feed_dict = {}

    def _add_entry(k: str, v: Any) -> None:
        feed_dict[f"{key_prefix}{k}"] = v
        if isinstance(v, dict):
            for inner_k, inner_v in v.items():
                _add_entry(f"{k}.{inner_k}", inner_v)

    for k, v in d.items():
        _add_entry(k, v)
    return feed_dict


def handle_io(
    catalog: Union[Dict[str, Any], Callable[[], Dict[str, Any]]],
    credentials: Union[None, Dict[str, Any], Callable[[], Dict[str, Any]]] = None,
    parameters: Union[None, Dict[str, Any], Callable[[], Dict[str, Any]]] = None,
    add_credentials_to_catalog: bool = False,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Decorate a function with kedro-powered IO.

    Handy for abstracting IO details from graph structure specification
    in non-kedro orchestrators (e.g. airflow).

    Parameters
    ----------
    catalog : dict or callable
        Dict representation of a Kedro data catalog to be passed to
        kedro.io.DataCatalog.from_config(); see kedro docs for details;
        if a callable is provided it should return the dict
    credentials : dict or callable, optional
        Dict representation of dataset credentials to be passed to
        kedro.io.DataCatalog.from_config(); see kedro docs for details;
        if a callable is provided it should return the dict
    parameters : dict or callable, optional
        Dictionary of parameters to be added to the materialized kedro
        DataCatalog using DataCatalog.add_feed_dict; see kedro docs for details;
        if a callable is provided it should return the dict
    add_credentials_to_catalog : bool, default=False
        If true, credentials will be included in the catalog and resolvable
        as inputs to a node.

    Returns
    -------
    callable
        A function for decorating another function with kedro-powered IO, using
        the provided catalog, credentials, and parameters. The decorated function
        has two required arguments, `inputs` and `outputs` that expect references
        to datasets in the catalog as when constructing a kedro node; see kedro
        docs for details.

        In addition, the decorated function has an optional keyword argument,
        `checkpoint` which, if provided, should name a TextDataSet in the catalog
        where a str checksum for function executions can be recorded. During runtime,
        if the checksum of the function inputs and sources matches the checksum
        in this dataset, the function execution will be skipped.

    Examples
    --------
    >>> my_catalog = {
    ...     "my_in_ds": {"type": "text.TextDataSet", "filepath": "my_in_ds.txt"},
    ...     "my_out_ds": {"type": "text.TextDataSet", "filepath": "my_out_ds.txt"},
    ...     "checksum_ds": {"type": "text.TextDataSet", "filepath": "checksum.txt"}
    ... }
    >>> my_credentials = {
    ...     "my_token": "bar"
    ... }
    >>> @handle_io(catalog=my_catalog, credentials=my_credentials, add_credentials_to_catalog=True)
    ... def foo(text: str, token: str):
    ...     return f"{text} bar"
    >>> foo(inputs=["my_in_ds", "my_token"] , outputs="my_out_ds", checkpoint="checksum_ds")
    """

    def wrapper_factory(f: Callable[..., Any]) -> Callable[..., Any]:
        def wrapper(
            inputs: Union[None, str, List[str], Dict[str, str]],
            outputs: Union[None, str, List[str], Dict[str, str]],
            checkpoint: Optional[str] = None,
        ) -> None:
            catalog_obj = DataCatalog.from_config(
                _resolve_dict(catalog), credentials=_resolve_dict(credentials)
            )
            if parameters is not None:
                catalog_obj.add_feed_dict(
                    get_feed_dict(
                        _resolve_dict(parameters), copy_dict_as="parameters", key_prefix="params:"
                    )
                )
            if add_credentials_to_catalog and credentials is not None:
                catalog_obj.add_feed_dict(
                    get_feed_dict(
                        _resolve_dict(credentials), copy_dict_as=None, key_prefix="credentials:"
                    )
                )
            f_args, f_kwargs = _build_inputs(inputs, catalog_obj)

            if checkpoint is not None:
                checksum = ""
                try:
                    checksum = get_hash(f, *f_args, **f_kwargs)
                    prior_checksum = catalog_obj.load(checkpoint)
                    assert prior_checksum == checksum
                    return
                except Exception:
                    pass
                rv = f(*f_args, **f_kwargs)
                _persist_outputs(rv, outputs, catalog_obj)
                catalog_obj.save(checkpoint, checksum)
            else:
                rv = f(*f_args, **f_kwargs)
                _persist_outputs(rv, outputs, catalog_obj)

        return wrapper

    return wrapper_factory
