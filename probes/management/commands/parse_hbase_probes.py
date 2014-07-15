"""
Parse output from HBase probes tool (read from stdin).

It's output is tab-separated table with fields in every line:
1. server name
2. full region name
3. average query time in ms
"""
import sys
from probes import models
from probes.lib import process

from django.core.management.base import BaseCommand, CommandError


class Command (BaseCommand):
    fields_count = 3
    separator = '\t'

    def handle (self, *args, **options):
        probes_count = 0
        lines_skipped = 0

        processor = process.ProbesProcessor()

        for l in sys.stdin:
            try:
                l = l.strip()
                items = l.split(self.separator)
                if len(items) != self.fields_count:
                    lines_skipped += 1
                    self.stderr.write("Line with wrong fields count (get %d, %d expected),"
                                      " skipped\n" % (len(items), self.fields_count))
                    continue

                server, region, time = items

                # push probe data into our averaging system
                processor.process(server, region, long(time))
                probes_count += 1
            except Exception as e:
                self.stderr.write("Error process line, skipped: %s\n" % e)
                lines_skipped += 1

        # check servers for anomalies
        processor.check_servers ()
        self.stdout.write("Processed %d probes, %d lines skipped\n" % (probes_count, lines_skipped))
