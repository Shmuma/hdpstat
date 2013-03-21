"""
Parse listing of HBase root in HDFS
"""

import re
import sys
import datetime
import subprocess

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


    def dump (self, dt):
        table, created = models.Table.objects.get_or_create (name=self.name)
        sample, created = models.Sample.objects.get_or_create (date=dt)

        # wipe all samples for this table
        samples = models.TableSample.objects.filter (table=table, sample=sample)
        if samples.count () > 0:
            samples.delete ()

        regions = len (self.reg_size.keys ())
        avg = sum (self.reg_size.values ()) / float (regions)

        sum_hfiles = sum (self.reg_hfiles.values ())
        avg_hfiles = sum_hfiles / float (regions)
        max_hfiles = max (self.reg_hfiles.values ())

        tableSample = models.TableSample (table=table, sample=sample,
                                          size=self.size,
                                          regionSizeAvg=avg,
                                          regions=regions,
                                          oldestHFile=self.oldest,
                                          hfileCountMax=max_hfiles,
                                          hfileCountAvg=avg_hfiles,
                                          hfileCount=sum_hfiles)
        tableSample.save ()

        for cf_name in self.cf_size.keys ():
            cf, created = models.CF.objects.get_or_create (table=table, name=cf_name)
            
            samples = models.CFSample.objects.filter (cf=cf, sample=sample)
            if samples.count () > 0:
                samples.delete ()

            regions = len (self.cf_hfiles[cf_name].values ())
            sum_hfiles = sum (self.cf_hfiles[cf_name].values ())
            avg_hfiles = sum_hfiles / float (regions)
            max_hfiles = max (self.cf_hfiles[cf_name].values ())

            cfsample = models.CFSample (cf=cf, sample=sample,
                                        size=self.cf_size[cf_name],
                                        avgSize=self.cf_size[cf_name] / float (regions),
                                        hfileCountMax=max_hfiles,
                                        hfileCountAvg=avg_hfiles,
                                        hfileCount=sum_hfiles)
            cfsample.save ()


class Command (BaseCommand):
    def handle (self, *args, **options):
        data = self._get_data (args)
        tables = {}
        dt = datetime.datetime.now ().replace (tzinfo=utc)

        for l in data:
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
            t.dump (dt)


    def _get_data (self, args):
        if len (args) == 0:
            # run 'hadoop dfs -lsr /hbase' command and fetch it's output
            try:
                p = subprocess.Popen (["hadoop", "dfs", "-lsr", "/hbase"], stdout=subprocess.PIPE)
                stdout, stderr = p.communicate ()
                if p.returncode == 0:
                    return stdout.split ('\n')
            except OSError as e:
                print "Error %s" % e
            return []
        elif len (args) == 1:
            if args[0] == "-":
                data = sys.stdin.readlines ()
            else:
                with open (args[0]) as fd:
                    data = fd.readlines ()
            return data
        else:
            print "Usage: parse-hdfs.py [ - | hbase-listing.txt ]"
            return []


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
