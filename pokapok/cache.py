import os
from pathlib import Path
import shutil
import tempfile
from urllib.parse import urlparse

import requests


TEMP_DIR_PREFIX = 'pokapok-udal-'


class Directory():
    """
    Cache directory to store downloaded files.

    A temporary directory is used if no path to an existing directory is given.
    Any given path must exist and be writeable.
    """

    def __init__(self, path: str | Path | None = None, ):
        """
        Cache directory to store downloaded files.

        A temporary directory is used if no path to an existing directory is given.
        Any given path must exist and be writeable.

        Args:
            path: Path to the cache directory.
        """
        self._path = path
        self._tmp_dir = None

    def __enter__(self):
        if self._path is None:
            self._tmp_dir = tempfile.mkdtemp(prefix=TEMP_DIR_PREFIX)
        return self

    def __exit__(self, type, value, traceback):
        if self._tmp_dir is not None:
            shutil.rmtree(self._tmp_dir)

    def download(self,
            url: str,
            path: str | Path,
            mkdir: bool | None = None,
            filename: str | None = None
            ):
        """
        Download a file to the cache directory.

        Args:
            url: URL of the file to download.
            path: Path within the cache directory where to download the file to.
            mkdir: If provided and `True`, build the required parent directories for the downloaded file.
            filename: Name for the downloaded file. Defaults to the name in the URL if not provided.
        """
        dir = self._path or self._tmp_dir
        if dir is None:
            raise Exception('no directory to save download')
        dir = Path(dir).joinpath(path)
        if filename is None:
            filename = Path(urlparse(url).path).name
        file_path = dir.joinpath(filename)
        if os.path.exists(file_path):
            return file_path
        else:
            response = requests.get(url, stream=True)
            response.raise_for_status()
            if mkdir:
                os.makedirs(Path(file_path).parent.resolve(), exist_ok=True)
            with open(file_path, 'wb') as file:
                for chunk in response.iter_content(chunk_size=1024):
                    file.write(chunk)
            return file_path
