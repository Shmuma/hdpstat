import django_tables2 as tables

import datetime

class MillisecondColumn (tables.Column):
    """
    A column that renders milliseconds timedelta value into human-readable form
    """
    def render (self, value):
        return str (datetime.timedelta (seconds=long (value/1000)))



class TimedeltaColumn (tables.Column):
    """
    A column that renders timedeltas
    """
    def render (self, value):
        # get rid of microseconds
        td = datetime.timedelta (seconds=long (value.total_seconds ()))
        return str (td)


class LargeNumberColumn (tables.Column):
    """
    A column that renders large integer value into human-readable form (M, G, T, P, etc)
    """
    def __init__ (self, divider = 1.0, **extra):
        super (LargeNumberColumn, self).__init__ (**extra)
        self.divider = divider


    @staticmethod
    def best_suffix_and_mul (value, binary=True):
        # maps power of 10 to apropriate suffix
        if binary:
            base = 2

            power_to_suffix = {
                10: 'K',
                20: 'M',
                30: 'G',
                40: 'T',
                50: 'P',
                60: 'E',
                70: 'Z',
                80: 'Y',
                1000: 'inf'}
        else:
            base = 10
            power_to_suffix = {
                3: 'K',
                6: 'M',
                9: 'G',
                12: 'T',
                15: 'P',
                18: 'E',
                21: 'Z',
                24: 'Y',
                1000: 'inf'}           

        prev_power = 0
        prev_suffix = ''
        for power in sorted (power_to_suffix.keys ()):
            if value < pow (base, power):
                return (1.0 / pow (base, prev_power), prev_suffix)
            prev_power = power
            prev_suffix = power_to_suffix[power]       


    @staticmethod
    def format (value):
        mul, suff = LargeNumberColumn.best_suffix_and_mul (value)
#        print value, mul, float (value) * mul
        return "%.2f%s" % (float (value) * mul , suff)


    def render (self, value):
        if self.divider != 0:
            value /= float (self.divider)
        return LargeNumberColumn.format (value)
