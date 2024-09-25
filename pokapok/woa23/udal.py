from pathlib import Path
import os.path
import tempfile
import requests
from typing import Any, List
import xarray

from ..broker import Broker
from ..cache import Directory
from ..config import Config
from ..namedqueries import NamedQueryInfo, QueryName, QUERY_NAMES, QUERY_REGISTRY
from ..result import Result
from .types import Decade, TimeRes, Variable


localBrokerQueryNames: List[QueryName] = [
    'urn:pokapok:udal:woa23',
]


localBrokerQueries: dict[QueryName, NamedQueryInfo] = \
    { k: v for k, v in QUERY_REGISTRY.items() if k in localBrokerQueryNames }


class WOA23Broker(Broker):

    _config: Config

    _query_names: List[QueryName] = localBrokerQueryNames

    _queries: dict[QueryName, NamedQueryInfo] = localBrokerQueries

    def __init__(self, config: Config):
        self._config = config

    @property
    def queryNames(self) -> List[str]:
        return list(WOA23Broker._query_names)

    @property
    def queries(self) -> List[NamedQueryInfo]:
        return list(WOA23Broker._queries.values())

    def _execute_woa(self, params: dict[str, Any]):
        """World Ocean Atlas 2023 Data

        https://www.ncei.noaa.gov/access/world-ocean-atlas-2023/"""

        # variable
        variable: Variable | None = params.get('variable')
        if variable is None:
            raise Exception('missing variable')

        # decade
        decade: Decade | None = params.get('decade')
        if decade is None:
            raise Exception('decade not provided')
        # TODO decade validation
        #  - all (only oxygen, o2sat, o2utilization, silicate, phosphate, nitrate)
        #  - decav (only temperature, salinity)
        #  - decav71A0 (only temperature, salinity, oxygen, o2sat, o2utilization)
        #  - decav81B0 (only temperature, salinity)
        #  - decav91C0 (only temperature, salinity)
        #  - 5564 (only temperature, salinity)
        #  - 6574 (only temperature, salinity)
        #  - 7584 (only temperature, salinity)
        #  - 8594 (only temperature, salinity)
        #  - 95A4 (only temperature, salinity)
        #  - A5B4 (only temperature, salinity)
        #  - B5C2 (only temperature, salinity)

        # grid
        grid: int | None = params.get('grid')
        match grid:
            # TODO 0.25 for temperature and salinity
            case 0.25:
                if variable not in [Variable.Temperature, Variable.Salinity]:
                    raise Exception('1/4 deg grid only supported for temperature and salinity')
                file_grid_part = '04'
            case 1:
                file_grid_part = '01'
            case 5:
                file_grid_part = '5d'
            case _:
                raise Exception('invalid grid size; supported values: 1, 5')

        # longitude/latitude coordinates
        lon_min: float | None = params.get('lon_min')
        lon_max: float | None = params.get('lon_max')
        lat_min: float | None = params.get('lat_min')
        lat_max: float | None = params.get('lat_max')
        coords_are_none = [c == None for c in [lon_min, lon_max, lat_min, lat_max]]
        if (not all(coords_are_none) and any(coords_are_none)):
            raise Exception('partial coordinate arguments given')

        # time res
        time_res: TimeRes | None = params.get('time_res')
        if time_res is None:
            raise Exception('missing time_res')

        file_name = f'woa23_{decade.value}_{variable.short()}{time_res.value}_{file_grid_part}.nc'
        url = f'https://www.ncei.noaa.gov/thredds-ocean/fileServer/woa23/DATA/{variable.value}/netcdf/{decade.value}/{grid:0.2f}/{file_name}'

        # It is important to create a sub-directory for each variable to avoid
        # conflicts in case-insensitive file systems.
        with Directory(self._config.cache_dir) as dir:
            path = Path('woa23').joinpath(variable.value)
            file_path = dir.download(url, path, mkdir=True)
            dataset = xarray.open_dataset(file_path, decode_times=False)
            if all(coords_are_none):
                return dataset
            else:
                return dataset.sel(lon=slice(lon_min, lon_max), lat=slice(lat_min, lat_max))



    def execute(self, qn: QueryName, params: dict[str, Any] | None = None) -> Result:
        query = WOA23Broker._queries[qn]
        queryParams = params or {}
        match qn:
            case 'urn:pokapok:udal:woa23':
                return Result(query, self._execute_woa(queryParams))
            case _:
                if qn in QUERY_NAMES:
                    raise Exception(f'unsupported query name "{qn}"')
                else:
                    raise Exception(f'unknown query name "{qn}"')

