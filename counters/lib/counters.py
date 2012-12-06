"""
By counter tag (internal name), determine human-readable name and group.
"""

tagsClasses = { "TaskIO": set (["FILE_BYTES_READ", "HDFS_BYTES_READ", "FILE_BYTES_WRITTEN", "HDFS_BYTES_WRITTEN"]),
                "Time":   set (["MAP_WALL_CLOCK_MS", "REDUCE_WALL_CLOCK_MS", "SLOTS_MILLIS_MAPS", "SLOTS_MILLIS_REDUCES"]),
                "Mappers": set (["MAP_INPUT_RECORDS", "SPILLED_RECORDS", "MAP_OUTPUT_BYTES", "MAP_OUTPUT_RECORDS", "TOTAL_LAUNCHED_MAPS", "NUM_FAILED_MAPS"]),
                "Reducers": set (["REDUCE_SHUFFLE_BYTES", "REDUCE_INPUT_RECORDS", "REDUCE_OUTPUT_RECORDS", "TOTAL_LAUNCHED_REDUCES", "NUM_FAILED_REDUCES"]),
                "HBase:Put": set (["PUT_CALLS", "PUT_KVS", "PUT_BYTES", "PUT_MS"]),
                "HBase:Del": set (["DELETE_CALLS", "DELETE_MS"]),
                "HBase:Get": set (["GET_CALLS", "GET_KVS", "GET_BYTES", "GET_MS"]),
                "HBase:Scan": set (["SCAN_NEXT_MS", "SCAN_NEXT_COUNT", "SCAN_ROWS", "SCAN_KVS", "SCAN_BYTES"]),
                "HBase:ScanRS": set (["SCAN_DISK_BYTES", "SCAN_READ_MS"]),
                }


tagsNames = { "FILE_BYTES_READ": "Local data read, bytes",
              "FILE_BYTES_WRITTEN": "Local data written, bytes",
              "HDFS_BYTES_READ": "HDFS data read, bytes",
              "HDFS_BYTES_WRITTEN": "HDFS data written, bytes",
              
              "MAP_WALL_CLOCK_MS": "Mapper wall clock (ms)",
              "SHUFFLE_WALL_CLOCK_MS": "Shuffle wall clock (ms)",
              "SORT_WALL_CLOCK_MS": "Sort wall clock (ms)",
              "REDUCE_WALL_CLOCK_MS": "Reduce wall clock (ms)",
              "SLOTS_MILLIS_MAPS": "Total mappers slots time (ms)",
              "SLOTS_MILLIS_REDUCES": "Total reducers slots time (ms)",

              "TOTAL_LAUNCHED_MAPS": "Launcher maps",
              "NUM_FAILED_MAPS": "Failed maps",
              "TOTAL_LAUNCHED_REDUCES": "Launched reducers",
              "NUM_FAILED_REDUCES": "Failed reducers",

              "PUT_CALLS": "Put count",
              "PUT_KVS": "Put KVs",
              "PUT_BYTES": "Put bytes",
              "PUT_MS": "Put time (ms)",
              "DELETE_CALLS": "Delete count",
              "DELETE_MS": "Delete time (ms)",
              "GET_CALLS": "Get count",
              "GET_KVS": "Get KVs",
              "GET_BYTES": "Get bytes",
              "GET_MS": "Get time (ms)",
              "SCAN_NEXT_MS": "Scan next() time (ms)",
              "SCAN_NEXT_COUNT": "Scan next() calls",
              "SCAN_ROWS": "Scan rows",
              "SCAN_KVS": "Scan KVs",
              "SCAN_BYTES": "Scan bytes",
              "SCAN_DISK_BYTES": "Scan HFile bytes",
              "SCAN_READ_MS": "Scan HFile read time (ms)"
    }


def classify (counterTag):
    """
    Returns group name by counter tag
    """
    for g, tags in tagsClasses.iteritems ():
        if counterTag in tags:
            return g

    return None



def name (counterTag):
    """
    Returns human-readable name by counter tag: TODO
    """
    return tagsNames.get (counterTag, counterTag)
