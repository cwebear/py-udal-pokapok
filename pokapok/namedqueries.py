import typing
from typing import List, Literal


class NamedValue():
    """A field with a name in a parameter list or a result column."""

    def __init__(self, name: str):
        self._name = name

    @property
    def name(self) -> str:
        return self._name

    def as_dict(self) -> dict:
        return {
            'name': self._name,
        }


class TypedValue(NamedValue):
    """A named field and a description of its type."""

    def __init__(self, name: str, type: str):
        super().__init__(name)
        self._type = type

    @property
    def type(self) -> str:
        return self._type

    def as_dict(self) -> dict:
        return {
            'name': self._name,
            'type': self._type,
        }


class NamedQueryInfo():
    """Information about a named query, namely its name, parameters, and
    fields."""

    def __init__(self,
            name: str,
            params: List[NamedValue],
            fields: List[NamedValue]):
        self._name = name
        self._params = params
        self._fields = fields

    @property
    def name(self) -> str:
        return self._name

    @property
    def params(self) -> List[NamedValue]:
        return self._params

    @property
    def fields(self) -> List[NamedValue]:
        return self._fields

    def as_dict(self) -> dict:
        return {
            'name': self._name,
            'params': self._params,
            'fields': self._fields,
        }


QueryName = Literal[
    'urn:pokapok:udal:argo:list',
    'urn:pokapok:udal:argo:meta',
    'urn:pokapok:udal:argo:data',
    'urn:pokapok:udal:argo:files',
    'urn:pokapok:udal:woa23',
    ]


QUERY_NAMES: typing.Tuple[QueryName, ...] = typing.get_args(QueryName)


QUERY_REGISTRY : dict[QueryName, NamedQueryInfo] = {
    'urn:pokapok:udal:argo:list': NamedQueryInfo(
            'urn:pokapok:udal:argo:list',
            [
                TypedValue('float_mode', 'FloatMode|list[FloatMode]|None'),
                TypedValue('float_type', 'FloatType|list[FloatType]|None'),
                TypedValue('float', 'str|List[str]'),
                TypedValue('descending_cycles', 'bool'),
            ],
            [],
        ),
    'urn:pokapok:udal:argo:meta': NamedQueryInfo(
            'urn:pokapok:udal:argo:meta',
            [
                TypedValue('float', 'str|List[str]'),
            ],
            [],
        ),
    'urn:pokapok:udal:argo:data': NamedQueryInfo(
            'urn:pokapok:udal:argo:data',
            [
                TypedValue('dac', 'str'),
                TypedValue('float_mode', 'FloatMode|list[FloatMode]|None'),
                TypedValue('float_type', 'FloatType|list[FloatType]|None'),
                TypedValue('float', 'str'),
                TypedValue('descending_cycles', 'bool'),
            ],
            [],
        ),
    'urn:pokapok:udal:argo:files': NamedQueryInfo(
            'urn:pokapok:udal:argo:files',
            [
                TypedValue('dac', 'str'),
                TypedValue('float_mode', 'FloatMode|list[FloatMode]|None'),
                TypedValue('float_type', 'FloatType|list[FloatType]|None'),
                TypedValue('float', 'str'),
                TypedValue('descending_cycles', 'bool'),
                TypedValue('incl_meta', 'bool'),
                TypedValue('bypass_out_arch_building', 'bool'),
            ],
            [],
        ),
    'urn:pokapok:udal:woa23': NamedQueryInfo(
            'urn:pokapok:udal:woa23',
            [
                TypedValue('decade', 'Decade'),
                TypedValue('grid', 'float'),
                TypedValue('lat_max', 'float'),
                TypedValue('lat_min', 'float'),
                TypedValue('lon_max', 'float'),
                TypedValue('lon_min', 'float'),
                TypedValue('time_res', 'TimeRes'),
                TypedValue('variable', 'Variable'),
            ],
            [],
        ),
}
