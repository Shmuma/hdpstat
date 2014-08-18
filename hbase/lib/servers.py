import os

def regionservers (hbase_conf_root):
    if not os.path.exists(hbase_conf_root):
        return None
    res = []
    with open(os.path.join(hbase_conf_root, "regionservers")) as fd:
        for l in fd:
            l = l.strip()
            res.append(l)
    return res


