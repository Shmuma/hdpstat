import urllib2
import json
import datetime
import os

import hdfs
"""
Module provides routine to fetch, parse and store RS compaction state which can be obtained from
/rs-status?format=json&filter=general
"""

class CompactionState (object):
    def __init__ (self, rs, start_ts, region, store, state):
        self.rs = rs
        self.start_ts = start_ts
        self.start = datetime.datetime.fromtimestamp(start_ts)
        self.region = region
        self.store = store
        self.state = state

    def age(self):
        return datetime.datetime.now() - self.start

    def __str__(self):
        return "<CompactionState: rs=%s, start=%s, age=%s, reg=%s, store=%s, state=%s>" % (
            self.rs, self.start, self.age(), self.region, self.store, self.state)

    def __repr__(self):
        return str(self)

    @classmethod
    def from_json_obj(cls, rs, json):
        """
        Parse CompactionState object from parsed json object
        """
        descr = json["description"]
        dvals = descr.split(" ")
        reg, store = "", ""
        if dvals[0] == "Compacting" and len(dvals) == 4:
            store = dvals[1]
            reg = dvals[3]
        else:
            # flusing, open-close, etc
            return None

        return cls(rs, json['starttimems']/1000.0, reg, store, json["state"])
        


def fetch_compaction_json(server):
    url = "http://%s:60030/rs-status?format=json" % server
    resp = urllib2.urlopen(url)
    data = resp.read()
    resp.close()
    return data


def server_compactions(server):
    """
    Return list of compaction states currently performing by a server.
    In case of error, apropriate exception thrown
    """
    comp_json = fetch_compaction_json(server)
    data = json.loads(comp_json)
    res = []
    for obj in data:
        state = CompactionState.from_json_obj(server, obj)
        if state:
            res.append(state)
    return res



def region_compaction_path(region):
    """
    By region name get temporary compaction location
    """
    items = region.split(",")
    table = items[0]
    key = ",".join(items[1:-1])
    rest = items[-1]
    items = rest.split(".")
    h = items[1]
    return os.path.join("/hbase", table, h, ".tmp")



def paths_max_age(paths):
    """
    For list of HDFS paths get age of oldest file in that path.
    Return is a dict with path -> datetime.timedelta mapping
    """
    now = datetime.datetime.now()
    res = {path: datetime.timedelta(seconds=0) for path in paths}

    for line in hdfs.paths_listing(*paths):
        entry = hdfs.ListingEntry.parse(line)
        if entry:
            d = os.path.dirname(entry.name)
            if not d in res:
                logging.warn("Got listing entry not present in query %s", d)
            else:
                res[d] = max(res[d], now-entry.date)
    return res
