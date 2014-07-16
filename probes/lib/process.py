from probes import models

from sliding_averager import SlidingWindowAverager, SlidingWindowAveragerState

anomaly_threshold = 0.5
window_size = 20


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

        # averagers
        self.averager = SlidingWindowAverager(window_size)

        
    def process (self, server_name, region_name, time):
        """
        Entry point for probe data to system. Result is ProcessAnomaly instance or None is no anomaly detected
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
        region_time, created = models.HBase_RegionTime.objects.get_or_create(region=region)
        region_avg_state = region_time.averager_state
        avg_time = region_avg_state.value()
        
        if avg_time is not None and self.is_anomaly(avg_time, time):
            # handle anomaly
            msg="""
Request time is exceeded average plus threshold (%.2f%%). Values:
- region %s
- average time %.2f ms
- probe time %.2f ms
""" % (anomaly_threshold * 100.0, region_name, avg_time, time)
            result = ProcessAnomaly(is_region=True, object_name=region_name,
                                    text="Request time %.2f ms (avg is %.2f)" % (time, avg_time),
                                    description=msg)

        else:
            # Normal value, update state
            self.averager.update(time, region_avg_state)
            region_time.averager_state = region_avg_state
            region_time.save()
            result = None
        return result


    def is_anomaly(self, average, value):
        return value > average * (1.0 + anomaly_threshold)


    def check_servers (self):
        """
        Does final check of servers for anomalies, result is a list of ProcessResults
        """
        results = []

        for server_name in self.servers_sumtimes.keys():
            server = self._get_server(server_name)

            time = float(self.servers_sumtimes[server_name]) / self.servers_counts[server_name]
            server_time, created = models.HBase_ServerTime.objects.get_or_create(server=server)
            server_avg_state = server_time.averager_state
            avg_time = server_avg_state.value()

            if avg_time is not None and self.is_anomaly(avg_time, time):
                msg="""
Average server's probe time exceeded average + threshold (%.2f%%). Values:
- server: %s
- history response time %.2f ms
- probe time %.2f ms
""" % (anomaly_threshold * 100.0, server_name, avg_time, time)
                result = ProcessAnomaly(is_region=False, object_name=server_name,
                                        text="Request time %.2f ms (avg is %.2f)" % (time, avg_time),
                                        description=msg)
                results.append(result)
            else:
                # Normal value, update state
                self.averager.update(time, server_avg_state)
                server_time.averager_state = server_avg_state
                server_time.save()
        return results


    def _get_server (self, server):
        if server not in self.servers_cache:
            self.servers_cache[server], created = models.HBase_Server.objects.get_or_create(name=server)
        return self.servers_cache[server]            


    def _get_region (self, region):
        if region not in self.regions_cache:
            self.regions_cache[region], created = models.HBase_Region.objects.get_or_create(name=region)
        return self.regions_cache[region]


class ProcessAnomaly (object):
    def __init__ (self, is_region, object_name, text=None, description=None):
        self.is_region = is_region
        self.object_name = object_name
        self.text = text
        self.description = description

    
        
