from enum import Enum


class FloatMode(Enum):
    REAL_TIME = 'R'
    DELAYED = 'D'


class FloatType(Enum):
    CORE = ''
    BGC = 'B'
    SYNTHETIC = 'S'
