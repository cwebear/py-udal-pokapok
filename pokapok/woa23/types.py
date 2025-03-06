from enum import Enum


class Decade(Enum):
    DECADE_all = 'all'
    '''All'''
    DECADE_decav = 'decav'
    '''Averaged decades'''
    DECADE_decav71A0 = 'decav71A0'
    '''1971-2000 climate normal'''
    DECADE_decav81B0 = 'decav81B0'
    '''1981-2010 climate normal'''
    DECADE_decav91C0 = 'decav91C0'
    '''1991-2020 climate normal'''
    DECADE_5564 = '5564'
    '''1955-1964 years'''
    DECADE_6574 = '6574'
    '''1965-1974 years'''
    DECADE_7584 = '7584'
    '''1975-1984 years'''
    DECADE_8594 = '8594'
    '''1985-1994 years'''
    DECADE_95A4 = '95A4'
    '''1995-2004 years'''
    DECADE_A5B4 = 'A5B4'
    '''2005-2014 years'''
    DECADE_B5C2 = 'B5C2'
    '''2015-2022 years'''

    def __str__(self):
        return f"{self.value}"  


class TimeRes(Enum):
    Annual = '00'
    January = '01'
    February = '02'
    March = '03'
    April = '04'
    May = '05'
    June = '06'
    July = '07'
    August = '08'
    September = '09'
    October = '10'
    November = '11'
    December = '12'
    Winter = '13'
    Spring = '14'
    Summer = '15'
    Autumn = '16'

    def __str__(self):
        return f"{self.value}"  


class SpatialRes(Enum):
    quart_deg = 0.25
    one_deg = 1
    five_deg = 5

    def __str__(self):
        return f"{self.value}"  

class Variable(Enum):
    Temperature = 'temperature'
    '''Temperature (°C)'''
    Salinity = 'salinity'
    '''Salinity (unitless)'''
    DissolvedOxygen = 'oxygen'
    '''Dissolved Oxygen (µmol/kg)'''
    PercentOxygenSaturation = 'o2sat'
    '''Percent Oxygen Saturation (%)'''
    ApparentOxygenUtilization = 'AOU'
    '''Apparent Oxygen Utilization (µmol/kg)'''
    Silicate = 'silicate'
    '''Silicate (µmol/kg)'''
    Phosphate = 'phosphate'
    '''Phosphate (µmol/kg)'''
    Nitrate = 'nitrate'
    '''Nitrate (µmol/kg)'''

    def short(self):
        match self:
            case Variable.Temperature:
                return 't'
            case Variable.Salinity:
                return 's'
            case Variable.DissolvedOxygen:
                return 'o'
            case Variable.PercentOxygenSaturation:
                return 'O'
            case Variable.ApparentOxygenUtilization:
                return 'A'
            case Variable.Silicate:
                return 'i'
            case Variable.Phosphate:
                return 'p'
            case Variable.Nitrate:
                return 'n'

    def __str__(self):
        return f"{self.value}"  