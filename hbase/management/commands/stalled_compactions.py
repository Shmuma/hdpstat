import logging
import datetime

from django.core.management.base import BaseCommand, CommandError

from hbase.lib.servers import regionservers
from hbase.lib import compact

def setup_logging():
    logging.basicConfig (format="%(asctime)s: %(message)s", level=logging.INFO)

class Command (BaseCommand):
    hbase_conf_root = "/usr/local/hadoop/hbase-conf/hbase-conf-prd"
    stall_threshold = 60*60

    def handle (self, *args, **options):
        setup_logging()
        logging.info("Perform stalled compaction check")

        rses = regionservers(self.hbase_conf_root)
        if rses is None:
            logging.error("HBase configuration not found in %s", self.hbase_conf_root)
            return
        logging.info("Will query %d regionservers: [%s, %s, ...]", len(rses), rses[0], rses[1])

        stalled = []
        stall_delta = datetime.timedelta(seconds=self.stall_threshold)

        for rs in rses:
            states = compact.server_compactions(rs)
            for s in states:
                if s.age() > stall_delta:
                    logging.info("Stalled: %s", s)
                    stalled.append(s)
