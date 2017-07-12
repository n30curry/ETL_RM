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

import os
import sys
import time
import importlib
import itertools
from multiprocessing import Pool

from Lib.pyp import Num

from Lib.pyp_hybrid.PyMDB import PyMDB
from Lib.pyp_hybrid.PyTDB import PyTDB
from Lib.pyp import TDB

from Lib.ConfigurationLoadder import ConfigurationLoadder

MDB_HOST = "192.168.0.207"
MDB_PORT = 39950
MDB_TABLE = "v63xj_mdb_behaviour"

TDB_PORT = 9879
TDB_HOST = "192.168.0.195"
TDB_TABLE = "test"


class mdb2tdb(object):
    """docstring for mdb2tdb"""
    def __init__(self):
        super(mdb2tdb, self).__init__()
        
    def _mdblogin(self):

        self.mdb = PyMDB()
        self.mdb.login({"host":MDB_HOST,"port":MDB_PORT})
        self.mdb.use(MDB_TABLE)

    def _tdblogin(self):

        self.tdb = TDB.DB()
        self.tdb.login({"host":TDB_HOST,"port":TDB_PORT})
        self.tdb.use(TDB_TABLE)

    def login(self):

        self._mdblogin()
        self._tdblogin()

    def _range(self, procid, procnum):

        ranges = []
        for sid in self.mdb.shards(MDB_TABLE):
            count, service = Num.split(sid)
            piceid, serviceid = Num.split(service)

            if piceid % procnum != procid:
                continue
            ranges.append([Num.merge(1,service,1),sid])



        return ranges

    def traverse(self,mdb_dict):

        # timeaddress = {}

        nodes, addrs, times = [], [], []
 
        longitude = mdb_dict.get('longitude',0)
        latitude = mdb_dict.get('latitude',0)
        addr = Num.merge(int(longitude*111120),int(latitude*111120))

        timel = mdb_dict.get('timestamppy',0)
        # timel = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timel)) if timel != 0 else timel
        for node in mdb_dict['identitys']:

            nodes.append(node)
            addrs.append(addr)
            times.append(timel)
        return nodes,addrs,times


    def run(self, procid, procnum, checkpoint):

        ranges = self._range(procid, procnum)
        for secene in self.mdb.traverse(MDB_TABLE, ranges, check_path=checkpoint, int_step=10000):
            snode, saddr, stime = [], [], []
            for sec in secene[0]:

                try:
                    if sec['timestamppy'] == None and sec['longitude'] == None:
                        continue
                except:
                    continue

                nodes, addrs, times = self.traverse(sec)
                snode.extend(nodes)
                saddr.extend(addrs)
                stime.extend(times)
            self.tdb.inserts(TDB_TABLE, snode, saddr, stime)

def run_as_cmd(procid, procnum, checkpoint):

    m2t = mdb2tdb()
    m2t.login()
    m2t.run(procid, procnum, checkpoint)

if __name__ == "__main__":

    startid = int(sys.argv[1])
    endid = int(sys.argv[2])
    procnum = int(sys.argv[3])

    pool = Pool(processes = endid - startid)

    for procid in xrange(startid, endid):
        pool.apply_async(run_as_cmd, (procid, procnum, str(procid)))

        time.sleep(0.1)

    pool.close()
    pool.join()
