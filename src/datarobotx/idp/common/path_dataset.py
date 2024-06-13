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

from copy import deepcopy
import pathlib
from pathlib import Path, PurePosixPath
import shutil
import sys
import tempfile
from typing import Any, Dict, Optional, Union
import weakref

try:
    import fsspec
    from kedro.io.core import (
        AbstractVersionedDataset,
        DatasetError,
        Version,
        get_filepath_str,
        get_protocol_and_path,
    )
except ImportError as e:
    raise ImportError("Consider including kedro in your project requirements`") from e


# https://discuss.python.org/t/fixing-subclassing-in-pathlib/8983/16
if sys.platform == "win32":
    _PathBase = pathlib.WindowsPath
else:
    _PathBase = pathlib.PosixPath


class WeakReferencablePath(_PathBase):
    """weakref-friendly path.

    pathlib.Path, like many built-ins, can't be used by default with weakref.
    """

    pass


class PathDataset(AbstractVersionedDataset):  # type: ignore
    """Kedro connector for interacting with arbitrary files and directories.

    Accommodates full directories as well as  data that is not ideal to
    serialize via pickle, json or other tabular formats (e.g. huggingface
    cache dir, directory of PDFs, audio files or video files)

    Transfers the provided pathlib.Path or tempfile.TemporaryDirectory
    to `filepath` using the normal fsspec mechanisms.

    When loading, a temporary directory is created locally containing the
    requested directory or file. In the case of multiple files, a pathlib.Path
    to this directory is returned. Otherwise, a pathlib.Path is returned.
    The temporary directory is cleaned up when the Path is garbage
    collected.

    Note that the path dataset is not recommended for most use cases.  It is
    intended for use cases where the data is not easily serialized and
    an archive from ArchiveDataset is not suitable.
    """

    def __init__(
        self,
        filepath: str,
        version: Version = None,  # type: ignore
        credentials: Optional[Dict[str, Any]] = None,
        fs_args: Optional[Dict[str, Any]] = None,
    ) -> None:
        _fs_args = deepcopy(fs_args) or {}
        _credentials = deepcopy(credentials) or {}

        protocol, path = get_protocol_and_path(filepath, version)
        if protocol == "file":
            _fs_args.setdefault("auto_mkdir", True)

        self._protocol = protocol
        self._fs = fsspec.filesystem(self._protocol, **_credentials, **_fs_args)

        super().__init__(
            filepath=PurePosixPath(path),
            version=version,
            exists_function=self._fs.exists,
            glob_function=self._fs.glob,
        )

    def _describe(self) -> Dict[str, Any]:
        return {
            "filepath": self._filepath,
            "protocol": self._protocol,
            "version": self._version,
        }

    def _load(self) -> Path:
        load_path = get_filepath_str(self._get_load_path(), self._protocol)
        temp_dir = tempfile.mkdtemp()

        if self._fs.isfile(load_path):
            self._fs.get(load_path, temp_dir, recursive=True)
            local_path = WeakReferencablePath(temp_dir + "/" + load_path.split("/")[-1])
        else:
            self._fs.get(load_path + "/*", temp_dir, recursive=True)
            local_path = WeakReferencablePath(temp_dir)
        weakref.finalize(
            local_path, shutil.rmtree, temp_dir
        )  # clean up temp dir on garbage collection

        return local_path

    def _save(
        self,
        data: Union[Path, str, tempfile.TemporaryDirectory[Any], bytes],
    ) -> None:
        f = None
        if isinstance(data, str):
            path = Path(data)
        elif isinstance(data, Path):
            path = data
        elif isinstance(data, tempfile.TemporaryDirectory):
            path = Path(data.name)
        elif isinstance(data, bytes):
            f = tempfile.NamedTemporaryFile(
                delete=False
            )  # delete must be False on windows to allow copying
            f.write(data)
            f.flush()
            path = Path(f.name)

        assert path.is_file() or path.is_dir(), "Provided path must be a file or directory."

        if not path.exists():
            raise ValueError("The provided path must be a path to a directory or file that exists.")

        save_path = get_filepath_str(self._get_save_path(), self._protocol)
        if path.is_file():
            self._fs.copy(str(path), save_path)
        else:
            self._fs.copy(str(path / "*"), save_path, recursive=True)

        if f is not None:
            # cleanup NamedTemporaryFile
            f.close()
        self._invalidate_cache()

    def _exists(self) -> bool:
        try:
            load_path = get_filepath_str(self._get_load_path(), self._protocol)
        except DatasetError:
            return False

        return bool(self._fs.exists(load_path))

    def _release(self) -> None:
        super()._release()
        self._invalidate_cache()

    def _invalidate_cache(self) -> None:
        """Invalidate underlying filesystem caches."""
        filepath = get_filepath_str(self._filepath, self._protocol)
        self._fs.invalidate_cache(filepath)
