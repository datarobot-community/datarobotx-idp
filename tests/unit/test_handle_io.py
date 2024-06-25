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

# from kedro.io import DataCatalog
# import pytest

# from datarobotx.idp.common.handle_io import handle_io
# from datarobotx.idp.common.hashing import get_hash


# @pytest.fixture(params=["none-output", "str-output", "list-output", "dict-output"])
# def output_type(request):
#     return request.param


# @pytest.fixture()
# def expected_output_data(output_type):
#     if output_type == "str-output":
#         return {"foo_out_ds": "foo_data"}
#     elif output_type in ["list-output", "dict-output"]:
#         return {"foo_out_ds": "foo_data", "bar_out_ds": "bar_data"}
#     else:
#         return {}


# @pytest.fixture()
# def dummy_output_rv(output_type):
#     if output_type == "none-output":
#         return None
#     elif output_type == "str-output":
#         return "foo_data"
#     elif output_type == "dict-output":
#         return {"foo_output_key": "foo_data", "bar_output_key": "bar_data"}
#     elif output_type == "list-output":
#         return ["foo_data", "bar_data"]


# @pytest.fixture()
# def dummy_output_maps(output_type):
#     if output_type == "none-output":
#         return None
#     elif output_type == "str-output":
#         return "foo_out_ds"
#     elif output_type == "dict-output":
#         return {"foo_output_key": "foo_out_ds", "bar_output_key": "bar_out_ds"}
#     elif output_type == "list-output":
#         return ["foo_out_ds", "bar_out_ds"]


# @pytest.fixture(
#     params=[
#         "none-input",
#         "str-input",
#         "list-input",
#         "dict-input",
#     ]
# )
# def input_type(request):
#     return request.param


# @pytest.fixture()
# def expected_input_data(input_type, expected_parameter_values):
#     if input_type == "none-input":
#         return (), {}
#     elif input_type == "str-input":
#         return ("foo_input_data",), {}
#     elif input_type == "list-input":
#         l = ["foo_input_data", "bar_input_data"]
#         l += list(expected_parameter_values.values())
#         return tuple(l), {}
#     elif input_type == "dict-input":
#         return (), {"foo_input_key": "foo_input_data", "bar_input_key": "bar_input_data"}


# @pytest.fixture()
# def dummy_input_maps(input_type, expected_parameter_values):
#     if input_type == "none-input":
#         return None
#     elif input_type == "str-input":
#         return "foo_ds"
#     elif input_type == "dict-input":
#         return {"foo_input_key": "foo_ds", "bar_input_key": "bar_ds"}
#     elif input_type == "list-input":
#         l = ["foo_ds", "bar_ds"]
#         l += list(expected_parameter_values.keys())
#         return l


# @pytest.fixture()
# def captured_inputs():
#     return []


# @pytest.fixture()
# def call_count():
#     return [0]


# @pytest.fixture
# def dummy_f(captured_inputs, dummy_output_rv, call_count):
#     def f(*args, **kwargs):
#         captured_inputs.append(args)
#         captured_inputs.append(kwargs)
#         call_count[0] += 1
#         return dummy_output_rv

#     return f


# @pytest.fixture
# def dummy_f_mutated(captured_inputs, dummy_output_rv, call_count):
#     def f(*args, **kwargs):
#         foo = "bar"  # NOQA
#         captured_inputs.append(args)
#         captured_inputs.append(kwargs)
#         call_count[0] += 1
#         return dummy_output_rv

#     return f


# @pytest.fixture
# def catalog_config(tmp_path):
#     foo_path = tmp_path / "foo.txt"
#     foo_path.write_text("foo_input_data")
#     bar_path = tmp_path / "bar.txt"
#     bar_path.write_text("bar_input_data")
#     foo_out_path = tmp_path / "foo_out.txt"
#     foo_out_path.write_text("")
#     bar_out_path = tmp_path / "bar_out.txt"
#     bar_out_path.write_text("")
#     return {
#         "foo_ds": {
#             "type": "text.TextDataSet",
#             "filepath": str(foo_path.resolve()),
#         },
#         "bar_ds": {
#             "type": "text.TextDataSet",
#             "filepath": str(bar_path.resolve()),
#         },
#         "foo_out_ds": {
#             "type": "text.TextDataSet",
#             "filepath": str(foo_out_path.resolve()),
#         },
#         "bar_out_ds": {
#             "type": "text.TextDataSet",
#             "filepath": str(bar_out_path.resolve()),
#         },
#     }


# @pytest.fixture(params=["sha-missing", "sha-empty", "sha-present"])
# def checkpoint_file_status(request):
#     return request.param


# @pytest.fixture
# def checkpoint_path(tmp_path, checkpoint_file_status):
#     path = tmp_path / "checkpoint.txt"
#     if checkpoint_file_status == "sha-missing":
#         pass
#     elif checkpoint_file_status == "sha-empty":
#         path.write_text("")
#     elif checkpoint_file_status == "sha-present":
#         path.write_text(get_hash("foo"))
#     return path


# @pytest.fixture
# def catalog_config_with_checkpoint(tmp_path, catalog_config, checkpoint_path):
#     catalog_config.update(
#         {"checkpoint_ds": {"type": "text.TextDataSet", "filepath": str(checkpoint_path.resolve())}}
#     )
#     return catalog_config


# @pytest.fixture(params=["params-dict", "params-f"])
# def parameters(request):
#     d = {
#         "param_1": "param_value_1",
#         "param_2": {"param_3": "param_value_3", "param_4": "param_value_4"},
#         "param_5": {"param_6": {"param_7": "param_value_7"}},
#     }
#     if request.param == "params-dict":
#         return d
#     else:

#         def get_params():
#             return d

#         return get_params


# @pytest.fixture
# def expected_parameter_values(parameters):
#     if isinstance(parameters, dict):
#         d = parameters
#     else:
#         d = parameters()
#     return {
#         "parameters": d,
#         "params:param_1": "param_value_1",
#         "params:param_2": {"param_3": "param_value_3", "param_4": "param_value_4"},
#         "params:param_2.param_3": "param_value_3",
#         "params:param_2.param_4": "param_value_4",
#         "params:param_5.param_6.param_7": "param_value_7",
#     }


# def test_handle_io(
#     dummy_f,
#     dummy_input_maps,
#     dummy_output_maps,
#     catalog_config,
#     expected_output_data,
#     captured_inputs,
#     expected_input_data,
#     parameters,
# ):
#     rv = handle_io(
#         catalog=catalog_config,
#         parameters=parameters,
#     )(dummy_f)(
#         inputs=dummy_input_maps,
#         outputs=dummy_output_maps,
#     )
#     assert rv == None

#     captured_args, captured_kwargs = captured_inputs
#     expected_args, expected_kwargs = expected_input_data
#     assert captured_args == expected_args
#     assert captured_kwargs == expected_kwargs

#     catalog = DataCatalog.from_config(catalog_config)
#     for ds_name in expected_output_data:
#         assert catalog.load(ds_name) == expected_output_data[ds_name]


# def test_handle_io_with_checkpoint(
#     dummy_f,
#     dummy_f_mutated,
#     dummy_input_maps,
#     dummy_output_maps,
#     call_count,
#     catalog_config_with_checkpoint,
#     expected_output_data,
#     captured_inputs,
#     expected_input_data,
#     parameters,
# ):
#     f = handle_io(
#         catalog=catalog_config_with_checkpoint,
#         parameters=parameters,
#     )(dummy_f)
#     rv = f(
#         inputs=dummy_input_maps,
#         outputs=dummy_output_maps,
#         checkpoint="checkpoint_ds",
#     )
#     assert rv == None

#     captured_args, captured_kwargs = captured_inputs
#     expected_args, expected_kwargs = expected_input_data
#     assert captured_args == expected_args
#     assert captured_kwargs == expected_kwargs

#     catalog = DataCatalog.from_config(catalog_config_with_checkpoint)
#     for ds_name in expected_output_data:
#         assert catalog.load(ds_name) == expected_output_data[ds_name]
#     checkpoint_val = catalog.load("checkpoint_ds")
#     assert len(checkpoint_val)

#     # call again
#     assert call_count[0] == 1
#     f(
#         inputs=dummy_input_maps,
#         outputs=dummy_output_maps,
#         checkpoint="checkpoint_ds",
#     )
#     assert call_count[0] == 1
#     for ds_name in expected_output_data:
#         assert catalog.load(ds_name) == expected_output_data[ds_name]
#     checkpoint_val_2 = catalog.load("checkpoint_ds")
#     assert checkpoint_val_2 == checkpoint_val

#     # simulated modifying f slightly, call a third time
#     handle_io(
#         catalog=catalog_config_with_checkpoint,
#         parameters=parameters,
#     )(dummy_f_mutated)(
#         inputs=dummy_input_maps,
#         outputs=dummy_output_maps,
#         checkpoint="checkpoint_ds",
#     )
#     assert call_count[0] == 2
#     checkpoint_val_3 = catalog.load("checkpoint_ds")
#     assert checkpoint_val_3 != checkpoint_val


# def test_add_credentials_to_catalog(catalog_config):
#     credentials = {"foo": {"bar1": "foobar", "bar2": "foobar"}}
#     captured_inputs = {}

#     def f_orig(**kwargs):
#         captured_inputs.update(kwargs)
#         return ""

#     f = handle_io(catalog=catalog_config, credentials=credentials, add_credentials_to_catalog=True)(
#         f_orig
#     )
#     f(inputs={"foo": "credentials:foo"}, outputs=list(catalog_config.keys())[0])
#     assert captured_inputs["foo"] == credentials["foo"]

#     captured_inputs.clear()
#     f(inputs={"foo": "credentials:foo.bar1"}, outputs=list(catalog_config.keys())[0])
#     assert captured_inputs["foo"] == credentials["foo"]["bar1"]
