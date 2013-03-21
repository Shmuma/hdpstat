"""
Parse listing of HBase root in HDFS
"""

import re
import sys
import datetime

from django.core.management.base import BaseCommand
from django.utils.timezone import utc
from tables import models


class TableData (object):
    @staticmethod
    def is_special_table (name):
        """
        Checks that given table name is special
        """
        return name in ["-ROOT-", ".META.", ".corrupt", ".logs", ".oldlogs"]

    @staticmethod
    def is_special_cf (name):
        return name in [".oldlogs", "recovered.edits", ".tmp"]


    def __init__ (self, name):
        self.name = name
        self.size = 0
        self.reg_size = {}
        self.oldest = None
        self.reg_hfiles = {}
        self.cf_size = {}
        self.cf_hfiles = {}

    
    def hfile (self, data):
        regname = data['reg']
        self.size += data['size']
        self.reg_size[regname] = self.reg_size.get (regname, 0) + data['size']
        self.reg_hfiles[regname] = self.reg_hfiles.get (regname, 0) + 1
        if self.oldest == None:
            self.oldest = data['date']
        elif data['date'] < self.oldest:
            self.oldest = data['date']

        cfname = data['cf']
        self.cf_size[cfname] = self.cf_size.get (cfname, 0) + data['size']
        if not cfname in self.cf_hfiles:
            self.cf_hfiles[cfname] = {}
        self.cf_hfiles[cfname][regname] = self.cf_hfiles[cfname].get (regname, 0) + 1


    def reg (self, data):
        regname = data['reg']
        self.reg_size[regname] = self.reg_size.get (regname, 0)
        self.reg_hfiles[regname] = self.reg_hfiles.get (regname, 0)


    def dump (self):
#        table, created = models.Table.objects.get_or_create (name=dat['table'])
#        sample, created = models.Sample.objects.get_or_create (date=dat['date'])

        regions = len (self.reg_size.keys ())
        avg = sum (self.reg_size.values ()) / float (regions)
        print "Name = %s, size = %d, regs = %d, avg_reg = %.2f, oldest = %s" % (self.name, self.size,
                                                                                regions, avg, self.oldest)
        sum_hfiles = sum (self.reg_hfiles.values ())
        avg_hfiles = sum_hfiles / float (regions)
        print "HFiles = %d, MaxHFiles = %d, AvgHFiles = %.2f" % (sum_hfiles, max (self.reg_hfiles.values ()), avg_hfiles)

        for cf in self.cf_size.keys ():
            regions = len (self.cf_hfiles[cf].values ())
            sum_hfiles = sum (self.cf_hfiles[cf].values ())
            avg_hfiles = sum_hfiles / float (regions)
            print "%s size=%d, avgSize=%.2f, HFiles=%d, MaxHFiles = %d, AvgHFiles = %.2f" % (
                cf, self.cf_size[cf], self.cf_size[cf] / float (regions), sum_hfiles,
                max (self.cf_hfiles[cf].values ()), avg_hfiles)

class Command (BaseCommand):
    def handle (self, *args, **options):
        tables = {}

        for l in sys.stdin:
            l = l.strip ()
            vals = self._parse_line (l)
            if vals == None:
                continue
            tname = vals['table']
            if not tname in tables:
                tables[tname] = TableData (tname)

            if vals['kind'] == 'reg':
                tables[tname].reg (vals)
            elif vals['kind'] == 'hfile':
                tables[tname].hfile (vals)

        for t in tables.values ():
            t.dump ()


    def _parse_line (self, l):
        ls = re.split (" +", l)
        if len (ls) != 8:
            return None
        # we don't need dirs
        if ls[0][0] == 'd':
            return None
        res = {'size': int (ls[4])}
        res['date'] = datetime.datetime.strptime (ls[5], "%Y-%m-%d").date ()
        name = ls[7]
        parts = name.split ('/')
        if len (parts) < 5:
            return None
#        print len (parts)
        res['name'] = name
        res['table'] = parts[2]
        res['reg'] = parts[3]
        res['cf'] = parts[4]
        if TableData.is_special_table (res['table']):
            return None
        if TableData.is_special_cf (res['cf']):
            return None
#        print len (parts), parts
        if len (parts) == 5:
            res['kind'] = 'reg'
        elif len (parts) == 6:
            res['kind'] = 'hfile'
        else:
            return None
        return res