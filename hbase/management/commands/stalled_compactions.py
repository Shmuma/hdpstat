import logging
import datetime

from django.core.management.base import BaseCommand, CommandError

from hbase.lib.servers import regionservers
from hbase.lib import compact

def setup_logging():
    logging.basicConfig (format="%(asctime)s: %(message)s", level=logging.INFO)

class Command (BaseCommand):
    hbase_conf_root = "/usr/local/hadoop/hbase-conf/hbase-conf-prd"
    long_threshold = 20*60
    stall_threshold = 20*60

    def handle (self, *args, **options):
        setup_logging()
        logging.info("Perform stalled compaction check")

        rses = regionservers(self.hbase_conf_root)
        if rses is None:
            logging.error("HBase configuration not found in %s", self.hbase_conf_root)
            return
        logging.info("Will query %d regionservers: [%s, %s, ...]", len(rses), rses[0], rses[1])

        long_compacts = []
        long_delta = datetime.timedelta(seconds=self.long_threshold)

        for rs in rses:
            states = compact.server_compactions(rs)
            for s in states:
                if s.age() > long_delta and s.state == "RUNNING":
                    logging.info("Long compact: %s", s)
                    long_compacts.append(s)

        if len(long_compacts) == 0:
            logging.info("No compactions longer than %s detected, exit", long_delta)
            return

        # check every long compact for stall conditions (take mtime from hdfs)
        paths = {}
        for comp in long_compacts:
            path = compact.region_compaction_path(comp.region)
            l = paths.get(path, [])
            l.append(comp)
            paths[path] = l

        logging.info("Need to check %d HBase paths for stalled compactions", len(paths))
        paths_ages = compact.paths_max_age(paths.keys()[:10])
        stall_delta = datetime.timedelta(seconds=self.stall_threshold)
        stall_rses = set()

        for path,age in paths_ages.iteritems():
            if age > stall_delta:               
                for comp in paths[path]:
                    stall_rses.add(comp.rs)

        logging.info("Stalled compactions found on %d regionservers: %s", len(stall_rses), ", ".join(sorted(stall_rses)))
