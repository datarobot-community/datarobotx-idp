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
import functools
import pathlib
from pathlib import Path, PurePosixPath
import shutil
import sys
import tarfile
import tempfile
from typing import Any, Dict, Optional, Union
import weakref
import zipfile

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


class ArchiveDataset(AbstractVersionedDataset):  # type: ignore
    """Kedro connector for persisting an archive file.

    Accommodates data that is not ideal to serialize via pickle, json or other tabular
    formats (e.g. huggingface cache dir, directory of PDFs, audio files or
    video files)

    Compresses the provided pathlib.Path or tempfile.TemporaryDirectory to a tar or zip
    file and transfers the archive to `filepath` using the normal fsspec mechanisms.

    When loading the archive, a pathlib.Path to a temporary directory containing the
    expanded archive is passed to the receiving function.
    """

    def __init__(
        self,
        filepath: str,
        version: Version = None,  # type: ignore
        credentials: Optional[Dict[str, Any]] = None,
        fs_args: Optional[Dict[str, Any]] = None,
    ) -> None:
        if filepath.endswith(".zip"):
            self._archive_format = "zip"
        elif filepath.endswith(".tar"):
            self._archive_format = "tar"
        elif filepath.endswith(".tar.gz"):
            self._archive_format = "gztar"
        else:
            raise ValueError("Filepath must end with .tar.gz, .tar, or .zip")
        _fs_args = deepcopy(fs_args) or {}
        _fs_open_args_load = _fs_args.pop("open_args_load", {})
        _fs_open_args_save = _fs_args.pop("open_args_save", {})
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

        _fs_open_args_load.setdefault("mode", "rb")
        _fs_open_args_save.setdefault("mode", "wb")
        self._fs_open_args_load = _fs_open_args_load
        self._fs_open_args_save = _fs_open_args_save

    def _describe(self) -> Dict[str, Any]:
        return {
            "filepath": self._filepath,
            "protocol": self._protocol,
            "version": self._version,
        }

    def _load(self) -> Path:
        load_path = get_filepath_str(self._get_load_path(), self._protocol)

        temp_dir = tempfile.mkdtemp()
        local_path = WeakReferencablePath(temp_dir)
        weakref.finalize(
            local_path, shutil.rmtree, temp_dir
        )  # clean up temp dir on garbage collection
        with self._fs.open(load_path, **self._fs_open_args_load) as fs_file:
            if self._archive_format == "zip":
                zip = zipfile.ZipFile(fs_file, mode="r")
                zip.extractall(local_path)
            else:
                tar = tarfile.open(fileobj=fs_file, mode="r")
                tar.extractall(local_path)
        return local_path

    def _save(self, data: Union[Path, str, tempfile.TemporaryDirectory[Any]]) -> None:
        if isinstance(data, str):
            path = Path(data)
        elif isinstance(data, tempfile.TemporaryDirectory):
            path = Path(data.name)
        else:
            path = data
        if not path.exists() or not path.is_dir():
            raise ValueError("The provided path must be a path to a directory that exists.")

        save_path = get_filepath_str(self._get_save_path(), self._protocol)
        temp_dir = tempfile.TemporaryDirectory()
        ext = "." + self._archive_format
        if ext == ".gztar":
            ext = ".tar.gz"
        shutil.make_archive(
            temp_dir.name + "/archive",
            self._archive_format,
            root_dir=path,
        )
        with open(temp_dir.name + "/archive" + ext, "rb") as tar_data:
            with self._fs.open(save_path, **self._fs_open_args_save) as fs_file:
                for chunk in iter(functools.partial(tar_data.read, 1024), b""):
                    fs_file.write(chunk)
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
