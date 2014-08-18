import re
import logging
import datetime
import subprocess

"""
HDFS-related routines - listing, parsing, etc
"""

def paths_listing(*paths):
    """
    Return output of 'hadoop dfs -ls -R' of paths as an array of strings
    """
    cmd = ["hadoop", "dfs", "-ls", "-R"] + list(paths)
    out = subprocess.check_output(cmd)
    return out.split('\n')


class ListingEntry (object):
    def __init__ (self, name, rights, replicas, user, group, size, date):
        self.name = name
        self.rights = rights
        self.replicas = replicas
        self.user = user
        self.group = group
        self.size = size
        self.date = date

    def __str__(self):
        return "<ListingEntry: name=%s, rights=%s, rep=%d, right=%s:%s, size=%d, date=%s>" % (
            self.name, self.rights, self.replicas, self.user, self.group, self.size, self.date)


    @classmethod
    def parse(cls, line):
        """
        Parse one listing line to ListingEntry
        """
        v = re.split("\s+", line)
        if len(v) != 8:
            logging.warn("hdfs.parse: Strange count of entries in listing - %d, expected 8. Line = %s",
                         len(v), line) 
            return None

        # ['-rw-r--r--', '3', 'hadoop', 'supergroup', '268435456', '2014-08-18', '20:15', '/hbase/file']
        date = datetime.datetime.strptime(v[5] + " " + v[6], "%Y-%m-%d %H:%M")

        return cls(name=v[7], rights=v[0], replicas=int(v[1]), user=v[2], group=v[3],
                   size=long(v[4]), date=date)

        
