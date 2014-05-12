import re
import os
import glob
import logging
import cPickle as pickle

"""
Watcher for two directories with running and complete jobs and tracks new and
changed files.
"""

class HadoopWatcher (object):   
    state_file = None

    running_dir = None
    done_dir = None

    # key is XML config name
    running_entries = {}
    done_entries = {}


    def __init__ (self, running_dir, done_dir, state_file = None):
        self.running_dir = running_dir
        self.done_dir = done_dir
        self.state_file = state_file
        self._restore_state ()


    def process (self):
        """
        Checks directories for new and modified files. Returns list of new/changed JobEnties
        """
        running = self._process_dir (self.running_dir, self.running_entries)
        done = self._process_dir (self.done_dir, self.done_entries, recurse=True)

        return running + done


    def checkpoint (self):
        """
        Save state - all entries processed by caller
        """
        self._save_state ()


    def _save_state (self):
        """
        Save state into state file
        """
        if self.state_file == None:
            return
        with open (self.state_file, "wb") as fd:
            pickle.dump (self.running_dir, fd)
            pickle.dump (self.done_dir, fd)
            pickle.dump (self.running_entries, fd)
            pickle.dump (self.done_entries, fd)


    def _restore_state (self):
        """
        Restore state from state file
        """
        if self.state_file == None:
            return

        if not os.path.exists (self.state_file):
            return

        with open (self.state_file, "rb") as fd:
            run_dir = pickle.load (fd)
            done_dir = pickle.load (fd)

            if run_dir != self.running_dir or done_dir != self.done_dir:
                logging.warn ("Saved state is for different directories, skip restore")
                return

            self.running_entries = pickle.load (fd)
            self.done_entries = pickle.load (fd)


    @staticmethod
    def _job_prefix (path):
        """
        Extracts job prefix from full file name
        """
        dname, fname = os.path.split (path)
        mo = re.search ("(([^_]+_){3})", fname)
        if mo:           
            prefix = mo.group (1)[:-1]
            return os.path.join (dname, prefix)
        return None


    def _process_dir (self, dir, entries, recurse=False):
        untouched = set (entries.keys ())
        fresh = []
        # find all files in directory, sort them and find xml-counter pair
        if recurse:
            files = []
            for path, dirnames, fnames in os.walk(dir):
                for f in fnames:
                    if f.startswith("job_"):
                        files.append(os.path.join(path, f))
        else:
            files = glob.glob ("%s/*" % dir)
        files.sort ()
        prev_name = None
        xml_name = None
        counter_name = None

        # first pass, collect prefixes of all xml files
        xml_prefix = set ()
        for f in files:
            if f.endswith ("_conf.xml"):
                prefix = self._job_prefix (f)
                if prefix:                   
                    xml_prefix.add (prefix)
        # second pass, find all counter names for which xml exists
        for f in files:
            if not f.endswith ("_conf.xml"):
                prefix = self._job_prefix (f)
                if prefix and prefix in xml_prefix:
                    xml_name = prefix + "_conf.xml"
                    counter_name = f
                    # process job entry
                    untouched.discard (xml_name)
                    if not xml_name in entries:
                        # new job entry, create it
                        try:
                            je = JobEntry (counter_name, xml_name)
                            fresh.append (je)
                            entries[xml_name] = je
                        except OSError:
                            pass
                    else:
                        # job entry exists, check for update
                        je = entries[xml_name]
                        if je.check ():
                            fresh.append (je)
        return fresh
        

    def findZombies (self, ttid):
        """
        Find and unlink all files in running directory which have different TT ID. Return list of their job IDs
        """
        zombies = set ()
        for f in glob.glob ("%s/*" % self.running_dir):
            if f.find ("_job_") == -1:
                continue
            rest = f.split ("_job_")[1]
            parts = ["job"] + rest.split ("_")
            
            tid = "_".join (parts[:2])
            jid = "_".join (parts[:3])
            
            if ttid != tid:
                zombies.add (jid)
                logging.info ("Removing zombie file: %s" % f)
                os.unlink (f)

        return zombies


class JobEntry (object):
    """
    Tracks state of job's files: counters and config
    """
    counter_file = None
    config_file = None

    counter_size = None
    config_size = None

    fresh = True

    def __init__ (self, counter_file, config_file):
        self.counter_file = counter_file
        self.config_file = config_file
        self.fresh = True

        self._update_state (os.stat (counter_file), os.stat (config_file))


    def __repr__ (self):
        return "<Counter: %s, %d, config: %s, %d>" % (os.path.basename (self.counter_file), self.counter_size, os.path.basename (self.config_file), self.config_size)


    def _update_state (self, counter_stat, config_stat):
        """
        Comapare files states and updates it
        """
        changed = counter_stat.st_size != self.counter_size or config_stat.st_size != self.config_size

        if changed:
            logging.info ("changed: %s (%s -> %s)" % (self.config_file, self.counter_size, counter_stat.st_size))
            self.counter_size = counter_stat.st_size
            self.config_size = config_stat.st_size
        return changed


    def check (self):
        if self.fresh:
            return True
        if self.exists ():
            if self._update_state (os.stat (self.counter_file), os.stat (self.config_file)):
                self.fresh = True
                return True
        return False


    def exists (self):
        return os.path.exists (self.counter_file) and os.path.exists (self.config_file)


    def processed (self):
        self.fresh = False
