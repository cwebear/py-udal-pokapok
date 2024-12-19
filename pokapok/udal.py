from .argo.udal import ARGO_URLS, ArgoBroker
from .woa23.udal import WOA23Broker
from .config import Config
from .namedqueries import QueryName
from .result import Result

import logging
# Get the logger for the library (it will use the root logger by default)
logger = logging.getLogger(__name__)
logger.propagate = True

class UDAL():
    """Uniform Data Access Layer"""

    def __init__(self, connectionString: str | None = None, config: Config | None = None):
        self._config = config or Config()
        if connectionString is None:
            self._broker = WOA23Broker(self._config)
        elif connectionString in ARGO_URLS:
            self._broker = ArgoBroker(connectionString, self._config)
        else:
            raise Exception(f'unsupported `connectionString` "{connectionString}"')

    def execute(self, urn: QueryName, params: dict | None = None) -> Result:
        return self._broker.execute(urn, params)

    @property
    def query_names(self):
        return self._broker.queryNames

    @property
    def queries(self):
        return self._broker.queries