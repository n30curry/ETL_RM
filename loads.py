#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
reload(sys)
sys.setdefaultencoding("utf-8")


# from Lib.pyp import Util
import os
import platform
import queryfz

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
from producter_consumer.productor import Productor
from producter_consumer.consumer import Consumer
import itertools


NDB_HOST = '192.168.0.203'
NDB_PORT = 9860
NDB_TABLE = 'test'
NUM = 10000
data = {
    "graph": {
        "nodes": [],
        "links": []
    },
    "keys": [],
    "sets": {}
}


class Loads():
    """docstring for Loads"""
    def __init__(self):

        
        self.ndb = NDB.DB()
        self.ndb.login({'host':NDB_HOST, 'port':NDB_PORT,'timeout':600})
        self.ndb.use(NDB_TABLE)
        print "ndb success"

        self.num = 0
    def graph(self):
        
        value = self.ndb.shards(NDB_TABLE)
        sids = value["list"]

        check = [0 for i in range(len(sids))]

        count = 10
        sids_count = map(lambda x: self.ndb.keyCount(NDB_TABLE, x), sids)
        sids_count.sort()
        self.ndb.qsort(NDB_TABLE)

        for i in xrange(sids_count[-1] / count + 1):
            # self.query1=queryfz.Query()
            keys = []
            rs = self.ndb.fetch(NDB_TABLE, sids, check, count)
            print "ndb.fetch"
            kids = rs["kids"]

            # print rs
            # time.sleep(1000)
            check = rs["check"]
            if check == [ 4294967295 for i in range(len(check))]:

                print "FINISH!!!"
                break
            # if kids == []:
            #     continue
            # else:
            #     print len(kids)
                # time.sleep(100)
            # self.query1.addKeys(kids)
            # print "addKeys"
            # data = self.datadeel(rs,self.query1)
            data = self.ndb.query(NDB_TABLE, rs['kids'],40,3,0)
            print "query success"
            # print "len(data)",len(data['nodes'])
            temp_count = len(data['nodes'])
            # try:
            #     if self.num/ len(data['nodes']) > 2:
            #         count = count * 2

            #     self.num = len(data['nodes'])
            # except:
            #     continue
            if self.num == 0:
                self.num = temp_count
            else:
                try:
                    if self.num / temp_count > 2:
                        count = count * 2
                except:
                    print "temp_count",temp_count
                    continue

            
            productor.produce(data)
            self.ndb.update(NDB_TABLE, data['nodes'])
            # check = rs["check"]


            # if check == [ 4294967295 for i in range(len(check))]:

            #     print "FINISH!!!"
            #     break


    def datadeel(self,fe,query):

        kids = fe["kids"]
        self.a = itertools.count()

        for i in range(5):
            if len(kids) == 0:
                break
            # print "*************"
            # print self.a.next()
            # print "*************"
            rs = self.ndb.query(NDB_TABLE, kids, 0)
            # print "query"
            a = query.parse(kids, rs["rids"], rs["nums"], rs["xkids"], rs["xnums"])
            # print "parse"
            kids = query.data["kids"]
            query.data["kids"] = []
            
        return query.data

        # b = self.ndb.maps(NDB_TABLE, query.data["graph"]["nodes"])

        # List = []

        # for index, value in enumerate(query.data["graph"]["nodes"]):
        #     c = index
        #     d = value
        #     e = b["rids"].index(index)
        #     f = b["keys"][e]
        #     List.append(f)
        # print "*************"
        # print "List",len(List),len(query.data["graph"]["nums"]),len(query.data["graph"]["links"]),len( query.data["graph"]["xnums"])
        # print "*************"
        # # print query.data
        # exit()
        # productor.produce(List)
        # t = NDB.graph(List, query.data["graph"]["nums"], query.data["graph"]["links"], query.data["graph"]["xnums"], 5,
        #               100)
        # print t
        # exit()
    def _graph(self,List,query):

        t = NDB.graph(List, query.data["graph"]["nums"], query.data["graph"]["links"], query.data["graph"]["xnums"], 5,
                      100)


    def consumer(self,List):
        pool = Pool(processes = 10)

        for procid in xrange(10):
            pool.apply_async(self.graph, (List, self.query1))

            time.sleep(0.1)

        pool.close()
        pool.join()



if __name__ == '__main__':

    begin = time.time()

    productor = Productor()
    consumer = Consumer()
    # query = queryfz.Query()
    loads = Loads()
    loads.graph()

    print "finished useing {}s".format(time.time() - begin)
