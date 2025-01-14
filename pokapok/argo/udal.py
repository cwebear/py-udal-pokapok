from pathlib import Path
from typing import Any
from urllib.parse import urljoin
from bs4 import BeautifulSoup
import re
import requests
import xarray
import time

from ..broker import Broker
from ..cache import Directory
from ..config import Config
from ..namedqueries import NamedQueryInfo, QueryName, QUERY_NAMES, QUERY_REGISTRY
from ..result import Result

from .data import cat_datasets
from .types import FloatMode, FloatType

import logging
# Get the logger for the library (it will use the root logger by default)
logger = logging.getLogger("qcv_ingester_log")


localBrokerQueryNames: list[QueryName] = [
    'urn:pokapok:udal:argo:meta',
    'urn:pokapok:udal:argo:data',
    'urn:pokapok:udal:argo:files',
]


localBrokerQueries: dict[QueryName, NamedQueryInfo] = \
    { k: v for k, v in QUERY_REGISTRY.items() if k in localBrokerQueryNames }

ARGO_URLS = [
    'https://data-argo.ifremer.fr',
    'https://usgodae.org/pub/outgoing/argo',
]


def _re_enum_options(enum) -> str:
    def value(e):
        if type(e) == str:
            return e
        else:
            return e.value
    values = [value(e) for e in enum]
    if any(map(lambda v: v == '', values)):
        option = '?'
    else:
        option = ''
    return '[' + ''.join(values) + f']{option}'


class ArgoBroker(Broker):

    _url: str
    _config: Config

    _query_names: list[QueryName] = localBrokerQueryNames

    _queries: dict[QueryName, NamedQueryInfo] = localBrokerQueries

    def __init__(self, url: str, config: Config):
        if url not in ARGO_URLS:
            raise Exception('Unsupported Argo URL')
        self._url = url
        self._config = config

    @property
    def queryNames(self) -> list[str]:
        return list(ArgoBroker._query_names)

    @property
    def queries(self) -> list[NamedQueryInfo]:
        return list(ArgoBroker._queries.values())

    @staticmethod
    def _argo_float_mode_type_re(float_mode: FloatMode|None, float_type: FloatType|list[FloatType]|None) -> str:
        # mode
        if float_mode == None or float_mode.lower() == "none" or float_mode.lower() == "all":
            mode = _re_enum_options(FloatMode)
        else:
            mode = float_mode.value
        # type
        if float_type == None:
            ftype = _re_enum_options(FloatType)
        elif type(float_type) == FloatType:
            ftype = float_type.value
        elif type(float_type) == list or type(float_type) == list[FloatType]:
            if len(float_type) == 1:
                ftype = float_type[0].value
            else:
                ftype = _re_enum_options(map(lambda ft: ft.value, float_type))
        else:
            raise Exception(f'invalid float type {type(float_type)}')
        return f'{ftype}{mode}'

    def _argo_float_url(self, dac: str, float: str):
        return f'{self._url}/dac/{dac}/{float}/'

    def _argo_float_profiles_url(self, dac: str, float: str):
        return f'{self._url}/dac/{dac}/{float}/profiles/'

    def _argo_file_name_re(self,
            float_mode: FloatMode | None,
            float_type: FloatType | list[FloatType] | None,
            include_descending_cycles: bool
            ):
        mt = ArgoBroker._argo_float_mode_type_re(float_mode, float_type)
        if include_descending_cycles:
            d = 'D?'
        else:
            d = ''
        return f'.*/{mt}([0-9]*)_([0-9]*){d}\\.nc$'
        
    def _find_the_dac(self, url, float):
        good_dac = None
        
        for dac in ["aoml", "bodc", "coriolis", "csio", "csiro", "incois", "jma" ,"kma", "kordi", "meds", "nmdis"]:         
            dac_url = f"{url}/{dac}"
            response = requests.get(dac_url)  
    
            if response.status_code != 200:
                logger.error(f"Error: Could not access {dac_url}")
                return []

            soup = BeautifulSoup(response.text, 'html.parser')        
            # Find all links in the directory listing
            hrefs = [a['href'] for a in soup.find_all('a')]
            
            if f"{float}/" in hrefs:
                good_dac = dac
                break

        if good_dac:
            return good_dac
        else:
            raise KeyError("no corresponding dac found --> exiting")

    def _web_file_urls(self, url: str) -> list[str]:
        # TODO Error handling.
        page = requests.get(url)
        soup = BeautifulSoup(page.content, 'html.parser')
        links = soup.find_all('a')
        links = filter(lambda l: l['href'] == l.text and not l.text.endswith('/'), links)
        links = map(lambda l: urljoin(url, l.text), links)
        return list(links)

    def _meta_file_urls(self, dac: str, float: str) -> list[str]:
        return [self._argo_float_url(dac, float) + f'{float}_meta.nc']

    def _file_urls(self, dac: str, float: str) -> list[str]:
        # TODO Error handling.
        return self._web_file_urls(self._argo_float_profiles_url(dac, float))

    def _filter_argo_float_files(self, float_mode, float_type, descending_cycles, float_files: list[str]) -> list[str]:
        file_re = re.compile(self._argo_file_name_re(float_mode, float_type, descending_cycles))
        argo_files = []
        for f in float_files:
            if file_re.match(f) is not None:
                argo_files.append(f)
        return argo_files

    def _try_to_dl_data(dl_line, **kwargs):
        all_files = kwargs.get(all_files) 
        url = kwargs.get(url) 
        path = kwargs.get(path)
        retries = 3
        for attempt in range(retries):
            try:
                logger.info(f"Attempt {attempt + 1} for file: {url}")
                all_files.append(str(dl_line))
                logger.info(f"Successfully downloaded: {url}")
                c+=1
                break  # Break out of the retry loop if successful
            
            except requests.exceptions.RequestException as req_err:
                logger.error(f"Attempt {attempt + 1} failed for {url} - Error: {req_err}")
                if attempt < retries - 1:
                    # If not the last attempt, retry after a brief pause
                    logger.info(f"Retrying in 40 seconds... (Attempt {attempt + 2}/{retries})")
                    time.sleep(40)
                else:
                    # If the final attempt fails, log the failure
                    logger.error(f"Failed to download {url} after {retries} attempts.")
                    pass
            except Exception as e:
                # Catch any other unforeseen errors
                logger.error(f"Unexpected error occurred for {url}: {e}")
                pass
    def _execute_argo_meta(self, params: dict[str, Any]):
        dac = params.get('dac')
        if dac == None:
            raise Exception('missing dac argument')
        float = params.get('float')
        if float == None:
            raise Exception('missing float argument')
        [url] = self._meta_file_urls(dac, float)
        result = None
        with Directory(self._config.cache_dir) as dir:
            meta_path = Path('argo', 'dac', dac, float)
            f = str(dir.download(url, meta_path, mkdir=True))
            meta = xarray.open_dataset(f)
            result = {
                'institution': meta.attrs.get('institution'),
                'title': meta.attrs.get('title'),
                'source': meta.attrs.get('source'),
                'references': meta.attrs.get('references'),
                'dimensions': list(meta.sizes.keys()),
                'variables': list(meta.variables.keys()),
            }
        return result

    def _execute_argo_data(self, params: dict[str, Any]):
        dac = params.get('dac')
        if dac == None:
            raise Exception('missing dac argument')
        float_mode = params.get('float_mode')
        float_type = params.get('float_type')
        float = params.get('float')
        if float == None:
            raise Exception('missing float argument')
        descending_cycles = params.get('descending_cycles')
        if descending_cycles == None:
            descending_cycles = True
        argo_file_urls = self._filter_argo_float_files(float_mode, float_type, descending_cycles, self._file_urls(dac, float))
        meta_file_urls = self._meta_file_urls(dac, float)
        all_files = []
        meta_path = Path('argo', 'dac', dac, float)
        profile_path = Path('argo', 'dac', dac, float, 'profiles')
        with Directory(self._config.cache_dir) as dir:
            for url in argo_file_urls:
                all_files.append(str(dir.download(url, profile_path, mkdir=True)))
            for url in meta_file_urls:
                all_files.append(str(dir.download(url, meta_path, mkdir=True)))
        results = cat_datasets([all_files])
        return results


    def _execute_argo_files(self, params: dict[str, Any]):
        
        # section = float mode
        float_mode = params.get('float_mode')
        
        # section = float type
        float_type = params.get('float_type')
        
        # section = float
        float = params.get('float')
        if float == None:
            raise Exception('missing float argument')
        
        # section = dac
        dac = params.get('dac')
        
        if dac :
            pass
        elif dac == "" or not dac:
            dac = self._find_the_dac(f"{self._url}/dac", float)
        else :
            raise Exception('missing dac argument, impossible to get from server...')
        
        # section descending_cycles
        descending_cycles = params.get('descending_cycles')
        if descending_cycles == None:
            descending_cycles = True
            
        argo_file_urls = self._filter_argo_float_files(float_mode, float_type, descending_cycles, self._file_urls(dac, float))
        meta_file_urls = self._meta_file_urls(dac, float)

        all_files = []
        
        if params.get('bypass_out_arch_building'):       
            profile_path=""
            meta_path=""
        else:
            meta_path = Path('argo', 'dac', dac, float)
            profile_path = Path('argo', 'dac', dac, float, 'profiles')
            
        
        with Directory(self._config.cache_dir) as dir:
            logger.info(f"start downloading meta file")
            if params.get('incl_meta'):
                for url in meta_file_urls:
                    all_files.append(str(dir.download(url, meta_path, mkdir=True)))
            logger.info(f"DL meta file : END !")
                
            logger.info(f"start downloading meta file")
            logger.info(f"{len(argo_file_urls)} files to DL.. Start !")
            c=1
            for url in argo_file_urls:
                logger.info(f"PROCESS file n° {c}/{len(argo_file_urls)}")
                retries = 3
                wait_s = 40
                for attempt in range(retries):
                    try:
                        # logger.info(f"Attempt {attempt + 1} for file: {url}")
                        all_files.append(str(dir.download(url, profile_path, mkdir=True)))
                        # logger.info(f"Successfully downloaded: {url}")
                        c+=1
                        break  # Break out of the retry loop if successful
                    
                    except requests.exceptions.RequestException as req_err:
                        logger.error(f"Attempt {attempt + 1} failed for {url} - Error: {req_err}")
                        if attempt < retries - 1:
                            # If not the last attempt, retry after a brief pause
                            logger.info(f"Retrying in {wait_s} seconds... (Attempt {attempt + 2}/{retries})")
                            time.sleep(wait_s)
                        else:
                            # If the final attempt fails, log the failure
                            logger.error(f"Failed to download {url} after {retries} attempts.")
                            pass
                    except Exception as e:
                        # Catch any other unforeseen errors
                        logger.error(f"Unexpected error occurred for {url}: {e}")
                        pass
                    
        logger.info(f" end downloads! youpi")
        
    def execute(self, qn: QueryName, params: dict[str, Any] | None = None) -> Result:
        query = ArgoBroker._queries[qn]
        queryParams = params or {}
        match qn:
            case 'urn:pokapok:udal:argo:meta':
                return Result(query, self._execute_argo_meta(queryParams))
            case 'urn:pokapok:udal:argo:data':
                return Result(query, self._execute_argo_data(queryParams))
            case 'urn:pokapok:udal:argo:files':
                return Result(query, self._execute_argo_files(queryParams))
            case _:
                if qn in QUERY_NAMES:
                    raise Exception(f'unsupported query name "{qn}"')
                else:
                    raise Exception(f'unknown query name "{qn}"')

    def test_argo_float_repo(self, params: dict[str, Any] | None = None) -> str:
        float = params.get('float')
        if float == None:
            raise Exception('missing float argument')
        try:
            dac = self._find_the_dac(f"{self._url}/dac", float)
        except:
            return
        url = self._argo_float_url(dac, float)
        return url