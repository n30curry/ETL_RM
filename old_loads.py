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



from Lib.pyp import NDB
from multiprocessing import Pool
import sys
import time


NDB_HOST = '127.0.0.1'
NDB_PORT = 9869
NDB_TABLE = 'test'
NUM = 10000


class Loads(object):
    """docstring for Loads"""
    def __init__(self):
        super(Loads, self).__init__()
        
        self.ndb = NDB.DB()
        self.ndb.login({'host':NDB_HOST, 'port':NDB_PORT})
        self.ndb.use(NDB_TABLE)
        print "ndb success"
    def run(self, procid, procnum):
        print "runing ......"
        ranges = self._range(procid, procnum)
        print ranges
        for piceid in ranges[1:]:
            print "************"
            keycount = self.ndb.keyCount(NDB_TABLE, piceid)
            print "************"
            print keycount
            if keycount > NUM:
                
                for i in range(keycount/NUM +1):

                    keyList = self.ndb.keyList(NDB_TABLE, piceid, i*NUM, NUM)['list']
                    select_dict = self.ndb.selects(NDB_TABLE, keyList)
                    graph_dict = self.reverse(select_dict)

                    node_list, link_list = [], []
                    for k,v in graph_dict.items():
                        node_list.append(k)
                        link_list.append(v)
                self.ndb.update(NDB_TABLE, select_dict['kids'])
                self.ndb.updateX(NDB_TABLE, node_list, link_list, select_dict['nums'])
                print "update success"
                # self.ndb.update(NDB_TABLE, select_dict['kids'], select_dict['xkids'], select_dict['nums'])
            else:
               
                keyList = self.ndb.keyList(NDB_TABLE, piceid, 0, keycount)['list']
                select_dict = self.ndb.selects(NDB_TABLE, keyList)
                # self.ndb.update(NDB_TABLE, select_dict['kids'], select_dict['xkids'], select_dict['nums'])
                graph_dict = self.reverse(select_dict)
                    
                node_list, link_list = [], []
                for k,v in graph_dict.items():
                    node_list.append(k)
                    link_list.append(v)
                self.ndb.update(NDB_TABLE, select_dict['kids'])
                self.ndb.updateX(NDB_TABLE, node_list, link_list, select_dict['nums'])
                print "update success"
                # self.ndb.update(NDB_TABLE, select_dict['kids'], select_dict['xkids'], select_dict['nums'])


    def reverse(self,select_dict):

        node_list, link_list = [], []
        kids = select_dict['kids']
        xkids = select_dict['xkids']
        for ipos, links in enumerate(xkids):
            for node in links:
                temp_list = []
                node_list.append(node)
                for ipos1, links1 in enumerate(xkids):
                    if node in links1:
                        temp_list.append(kids[ipos1])
                link_list.append(temp_list)

        graph_dict = dict(zip(node_list, link_list))

        return graph_dict
        # node_list, link_list = [], []
        # for k,v in graph_dict.items():
        #     node_list.append(k)
        #     link_list.append(v)


    def _range(self,procid,procnum):

        ranges = []
        for piceid in self.ndb.shards(NDB_TABLE)['list']:
            # count, service = Num.split(sid)
            # piceid, serviceid = Num.split(service)
            # print "piceid",piceid
            # print "procid",procid
            # print "procnum",procnum
            if piceid % procnum != procid:
                continue

            ranges.append(piceid)
        return ranges
            # print "append success"

def run_as_cmd(procid, procnum):

    loads = Loads()
    loads.run(procid, procnum)

if __name__ == '__main__':

    begin = time.time()
    startid = int(sys.argv[1])
    endid = int(sys.argv[2])
    procnum = int(sys.argv[3])

    pool = Pool(processes = endid - startid)

    for procid in xrange(startid, endid):
        pool.apply_async(run_as_cmd, (procid, procnum))

        time.sleep(0.1)

    pool.close()
    pool.join()

    print "finished useing {}s".format(time.time() - begin)
