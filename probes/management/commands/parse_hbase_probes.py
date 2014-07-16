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
                anomaly = processor.process(server, region, long(time))
                if anomaly:
                    self._handle_anomaly(anomaly)
                probes_count += 1
            except Exception as e:
                self.stderr.write("Error process line, skipped: %s\n" % e)
                alert = model.HBase_Alert(0, "Error", "Exception processing line '%s'"  % l,
                                          str(e))
                lines_skipped += 1

        # check servers for anomalies
        for anomaly in processor.check_servers ():
            self._handle_anomaly(anomaly)

        self.stdout.write("Processed %d probes, %d lines skipped\n" % (probes_count, lines_skipped))


    def _handle_anomaly (self, anomaly):
        if anomaly.is_region:
            kind = 2
        else:
            kind = 1
        alert = models.HBase_Alert(kind=kind, objname=anomaly.object_name,
                                   text=anomaly.text, description=anomaly.description)
        alert.save()
