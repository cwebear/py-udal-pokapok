from pathlib import Path


class Config:

    cache_dir: Path | None

    def __init__(self, cache_dir: str|Path|None = None):
        if cache_dir is None:
            self.cache_dir = None
        else:
            self.cache_dir = Path(cache_dir)
