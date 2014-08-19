import logging
import datetime

from optparse import make_option

from django.core.management.base import BaseCommand, CommandError

from hbase.lib.servers import regionservers
from hbase.lib import compact

def setup_logging():
    logging.basicConfig (format="%(asctime)s: %(message)s", level=logging.INFO)

class Command (BaseCommand):
    option_list = BaseCommand.option_list + (
        make_option("--conf-root", dest="conf_root",
                    default="/usr/local/hadoop/hbase-conf/hbase-conf-prd",
                    help="HBase configuration root path"),
        make_option("--alert", dest="alert",
                    action="store_true",
                    help="Turn on alerting to monitoring by saving text file with result"),
        make_option("--alert-path", dest="alert_path",
                    default="/var/tmp/stalled_compacts",
                    help="Text file with alert path"),
        make_option("--threshold", dest="threshold",
                    default=60,
                    help="How long compaction must have no progress to be considered 'stalled', in minutes. Default is 60",
                    type="int")
        )

    def handle (self, *args, **options):
        setup_logging()
        logging.debug("Perform stalled compaction check")

        rses = regionservers(options['conf_root'])
        if rses is None:
            logging.error("HBase configuration not found in %s", self.hbase_conf_root)
            return
        logging.info("Will query %d regionservers: [%s, %s, ...]", len(rses), rses[0], rses[1])

        long_compacts = []
        threshold = datetime.timedelta(minutes=options['threshold'])

        for rs in rses:
            states = compact.server_compactions(rs)
            for s in states:
                if s.age() > threshold and s.state == "RUNNING":
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
        paths_ages = compact.paths_max_age(paths.keys())
        stall_rses = set()

        for path,age in paths_ages.iteritems():
            if age > threshold:
                for comp in paths[path]:
                    stall_rses.add(comp.rs)

        logging.info("Stalled compactions found on %d regionservers: %s", len(stall_rses), ", ".join(sorted(stall_rses)))

        if options['alert']:
            with open(options['alert_path'], 'w+') as fd:
                if len(stall_rses) > 0:
                    fd.write("Stalled compactions on %d RSes: %s\n" % (len(stall_rses), ", ".join(sorted(stall_rses))))
