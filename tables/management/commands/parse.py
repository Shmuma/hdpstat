"""
Parse old-format reports from given sources
"""

import sys
import os
import re
import glob
import datetime

from django.core.management.base import BaseCommand
from django.utils.timezone import utc
from tables import models


class Command (BaseCommand):
    def handle (self, *args, **options):
        for name in args:
            if os.path.isdir (name):
                self._parse_dir (name)
            else:
                self._parse (name)


    def _parse_dir (self, name):
        print "process dir '%s'" % name        
        for n in glob.iglob (os.path.join (name, "*")):
            if os.path.isdir (n):
                self._parse_dir (n)
            else:
                self._parse (n)


    def _parse (self, name):
        if not os.path.exists (name):
            print "skip not existed '%s'" % name
            return
        dat = {}
        print "parse '%s'" % name

        ptable = {
            "Date: ": ("date", self._parse_date),
            "Subject: ": ("table", self._parse_table),
            "Average reg size:": ("regAvgSize", self._parse_size),
            "Total table size:": ("tableSize", self._parse_size),
            "Amount of regions:": ("regions", self._parse_int),
            "Spliting regions:": ("splits", self._parse_int)
            }

        # parser state
        in_cfs1 = False
        in_cfs2 = False

        with open (name, 'r') as fd:
            for l in fd:
                l = l.strip ()

                for k in ptable.keys ():
                    dst, fun = ptable[k]
                    if l.startswith (k):
                        ll = l[len (k):]
                        dat[dst] = fun (ll)
                        break

                if l == "Average CF sizes:":
                    in_cfs1 = True
                    dat['cfs'] = {}
                    continue

                if in_cfs1:
                    if len (l) == 0:
                        in_cfs1 = False
                    else:
                        m = re.search ("^(\w+)\s+(.*)$", l)
                        if m:
                            cf = m.group (1)
                            dat['cfs'][cf] = {'size': self._parse_size (m.group (2))}

                if l == "CFs files statistics:":
                    in_cfs2 = True
                    continue

                if in_cfs2:
                    if len (l) == 0:
                        in_cfs2 = False
                    else:
                        m = re.search ("^(\w+)\s+([\d.]+)\s+(\d+)$", l)
                        if m:
                            cf = m.group (1)
                            favg = float (m.group (2))
                            fmax = int (m.group (3))
                            dat['cfs'][cf]['filesAvg'] = favg
                            dat['cfs'][cf]['filesMax'] = fmax
        if dat['regions'] > 0:
            self._save_data (dat)


    def _save_data (self, dat):
#        print dat

        table, created = models.Table.objects.get_or_create (name=dat['table'])
        sample, created = models.Sample.objects.get_or_create (date=dat['date'])

        # wipe all samples for this table
        samples = models.TableSample.objects.filter (table=table, sample=sample)
        if samples.count () > 0:
            samples.delete ()

        tableSample = models.TableSample (table=table, sample=sample,
                                          size=dat['tableSize'],
                                          regionSizeAvg=dat['regAvgSize'],
                                          regions=dat['regions'],
                                          splits=dat.get ('splits'))
        tableSample.save ()
        
        # CFs
        for cf_name in dat['cfs'].keys ():
            cf_data = dat['cfs'][cf_name]
            cf, created = models.CF.objects.get_or_create (table=table, name=cf_name)
            
            samples = models.CFSample.objects.filter (cf=cf, sample=sample)
            if samples.count () > 0:
                samples.delete ()

            cfsample = models.CFSample (cf=cf, sample=sample,
                                        size=cf_data['size'],
                                        hfileCountMax=cf_data.get ('filesMax'),
                                        hfileCountAvg=cf_data.get ('filesAvg'))
            cfsample.save ()


    def _parse_date (self, l):
        m = re.search ("^(.*)\s+([+\d]+)\s+\(\w*\)$", l)
        s = m.group (1)
#        print s, m.group (2)
        dt = datetime.datetime.strptime (s, "%a, %d %b %Y %H:%M:%S")
        return dt.replace (tzinfo=utc)
        

    def _parse_table (self, l):
        m = re.search ("(\S+) stats$", l)
        return m.group (1)


    def _parse_size (self, l):
        m = re.search ("([\d.]+) ([KMGT]?)B", l)
        mult = {'': 1, 'K': 2**10, 'M': 2**20, 'G': 2**30, 'T': 2**40 }
        return int (float (m.group (1)) * mult[m.group(2)])


    def _parse_int (self, l):
        m = re.search ("\d+", l)
        return int (m.group (0))
