from probes import models

anomaly_threshold = 0.5


class ProbesProcessor (object):
    """
    Processes probes data with caching of objects
    """
    
    def __init__ (self):
        self.servers_cache = {}
        self.regions_cache = {}

        # maps server name to sum of times for this server
        self.servers_sumtimes = {}
        # maps server name to probes count
        self.servers_counts = {}
        
    def process (self, server_name, region_name, time):
        """
        Entry point for probe data to system. Result is ProcessResult instance
        """
        server = self._get_server(server_name)
        region = self._get_region(region_name)

        # put in lastprobe
        probe = models.HBase_LastProbe (server=server, region=region, time=time)
        probe.save()

        # save server time for later anomaly detection
        self.servers_sumtimes[server_name] = time + self.servers_sumtimes.get(server_name, 0)
        self.servers_counts[server_name]   = self.servers_counts.get(server_name, 0) + 1

        # check region for anomaly
        

    def check_servers (self):
        """
        Does final check of servers for anomalies
        """
        pass


    def _get_server (self, server):
        if server not in self.servers_cache:
            self.servers_cache[server], created = models.HBase_Server.objects.get_or_create(name=server)
        return self.servers_cache[server]            


    def _get_region (self, region):
        if region not in self.regions_cache:
            self.regions_cache[region], created = models.HBase_Region.objects.get_or_create(name=region)
        return self.regions_cache[region]


class ProcessResult (object):
    pass
