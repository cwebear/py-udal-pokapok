from enum import Enum


class FloatMode(Enum):
    ALL: str = 'all'
    REAL_TIME = 'R'
    DELAYED = 'D'

    def __str__(self):
        return f"{self.value}"  

class FloatType(Enum):
    CORE = ''
    BGC = 'B'
    SYNTHETIC = 'S'

    def __str__(self):
        return f"{self.value}"  
