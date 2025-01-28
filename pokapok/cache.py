import os
from pathlib import Path
import shutil
import tempfile
from urllib.parse import urlparse
import requests
import logging
from time import time

# Get the logger for the library (it will use the root logger by default)
logger = logging.getLogger("qcv_ingester_log")

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

    def download(self, url: str, path: str|Path, mkdir: bool|None = None, filename: str|None = None):
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

        # Start the download and compare the local file size during the request
        with requests.get(url, stream=True) as response:
            response.raise_for_status()
            remote_file_size = int(response.headers.get('Content-Length', 0))

            if file_path.exists():
                local_file_size = file_path.stat().st_size

                # If sizes match, assume the file is already fully downloaded
                if local_file_size == remote_file_size:
                    logger.info(f"{os.path.basename(path)} already dl, skip")
                    return file_path

                # Otherwise, delete the incomplete file
                file_path.unlink()

            # Download the file
            with open(file_path, 'wb') as file:
                for chunk in response.iter_content(chunk_size=1024):
                    if chunk:
                        file.write(chunk)
                    

        

        return file_path