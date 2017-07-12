#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
reload(sys)
sys.setdefaultencoding("utf-8")

import os
import platform

PYP_LIBRARY_PATH = './Lib/pyp'

str_platform_system = platform.system()
if str_platform_system == "Linux":

    bReload = True
    strLibPath = os.path.join(os.getcwd(), PYP_LIBRARY_PATH)

    if "LD_LIBRARY_PATH" not in os.environ:
        os.environ["LD_LIBRARY_PATH"] = strLibPath
    elif strLibPath not in os.environ["LD_LIBRARY_PATH"]:
        os.environ["LD_LIBRARY_PATH"] += ":" + strLibPath
    else:
        bReload = False

    if bReload is True:
        try:
            os.execv(sys.argv[0], sys.argv)
        except Exception as exc:
            print ('Failed re-exec:' + str(exc))
            sys.exit(1)

elif str_platform_system == "Windows":
    
    strLibPath = os.path.join(os.getcwd(), PYP_LIBRARY_PATH)
    
    if "PATH" not in os.environ:
        os.environ["PATH"] = strLibPath
    elif strLibPath not in os.environ["PATH"]:
        os.environ["PATH"] += ";" + strLibPath

from Lib.pyp_hybrid.PyKDB import PyKDB
from Lib.pyp import Num
from multiprocessing import Pool
import time


class traverse(object):
    """docstring for traverse"""
    def __init__(self):
        super(traverse, self).__init__()
        
        
    def login(self):
        self.kdb = PyKDB()
        self.kdb.login({'host':'192.168.0.208','port':9910,'timeout':600})
        self.kdb.use('kdb_rmlink')
        print "kdb success"
    def ranges(self, procid = 0, procnum = 1, serviceids = None):

        ranges = []
        for sid in self.kdb.shards('kdb_rmlink')["list"]:

            pieceid, serviceid = Num.split(sid)
            if serviceids is not None and serviceid not in serviceids:
                continue

            if pieceid % procnum != procid:
                continue

            docs = self.kdb.keyCount('kdb_rmlink', sid)
            ranges.append({
                    "sid" : sid,
                    "count" : [[0, docs.get("count", 0)]],
                    "xcount" : [[0, docs.get("xcount", 0)]],
                })

        return ranges
    def run(self,procid, procnum):

        ranges = self.ranges(procid, procnum)
        # print ranges
        # print "*****"
        for links in self.kdb.traverse('kdb_rmlink',ranges):

            pass

def run_as_cmd(procid, procnum):
    tra = traverse()
    tra.login()
    tra.run(procid,procnum)

if __name__ == '__main__':

    startid = int(sys.argv[1])
    endid = int(sys.argv[2])
    procnum = int(sys.argv[3])

    pool = Pool(processes = endid - startid)

    for procid in xrange(startid, endid):
        pool.apply_async(run_as_cmd, (procid, procnum))

        time.sleep(0.1)

    pool.close()
    pool.join()