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


from Lib.pyp import KDB
from Lib.pyp import NDB
from producter_consumer.consumer import Consumer
from multiprocessing import Pool
import multiprocessing

import time

CENTER_HOST = '192.168.0.12'
CENTER_PORT = 9913
CENTER_TABLE = "kdb_center"

SUB_HOST = "192.168.0.12"
SUB_PORT = 9915
SUB_TABLE = "kdb_subordinate"

NDB_HOST = "192.168.0.203"
NDB_PORT = 9860
NDB_TABLE = "test"

class Insert(object):
    """docstring for Insert"""
    def __init__(self):

        super(Insert, self).__init__()

        self.consumer = Consumer()

        self.kdb_center = KDB.DB()
        self.kdb_center.login({'host':CENTER_HOST,'port':CENTER_PORT,'timeout':600})
        self.kdb_center.use(CENTER_TABLE)

        self.kdb_sub = KDB.DB()
        self.kdb_sub.login({'host':SUB_HOST,'port':SUB_PORT,'timeout':600})
        self.kdb_sub.use(SUB_TABLE)

        self.ndb = NDB.DB()
        self.ndb.login({'host':NDB_HOST, 'port':NDB_PORT,'time':600})
        self.ndb.use(NDB_TABLE)

        # self.num = 0
        print "succ"
    def run(self):

        while True:
        
            try:
                # print "*********"
                data = self.consumer.consume()
                print len(data['nodes'])
            except KeyboardInterrupt as e:
                break
            # exit()
            if data != None:
                a = self.ndb.maps(NDB_TABLE, data["nodes"])

                # List = []
                # List = map(lambda x :self.ndb.maps(NDB_TABLE, ))
                # print "nodes",nodes
                # print "data",data["nodes"]
                # exit()
                for i in range(len(a["rids"])):
                    data["nodes"][a["rids"][i]] = a["keys"][i]
                # for index,value in enumerate(data["nodes"]):
                #     # c = index
                #     # d = value
                #     e = a["rids"].index(index)
                #     f = a["keys"][e]
                #     List.append(f)
                #     self.num += 1
                #     if self.num % 10000 == 0:
                #         print self.num
                    # index = nodes['rids'].index(value)
                    # List.append(nodes['keys'][index])

                # print "ready graph"

                t = NDB.graph(data["nodes"], data["nums"], data["links"], data["xnums"], 50,
                      100,150)
                # print len(t['keys'])
                # time.sleep(10000)
                # print "ready insert"
                # print "t",t
                # exit()
                kdb_center_list = []
                kdb_sub_list = []
                for ipos, node in enumerate(t['keys']):

                    kdb_center_list.append(node + '\0' + node)
                    kdb_sub_list.append(node + '\0' + node)
                    if t['xkeys'][ipos] != []:
                        for i in t['xkeys'][ipos]:
                            kdb_center_list.append(node + '\0' + i)
                            kdb_sub_list.append(i + '\0' + node)
                # print kdb_center_list[0:100]
                try:
                    self.kdb_center.inserts(CENTER_TABLE, kdb_center_list, [])
                    self.kdb_sub.inserts(SUB_TABLE, kdb_sub_list, [])
                except:
                    continue
        




        
import traceback

def error(msg, *args):
    return multiprocessing.get_logger().error(msg, *args)

class LogException(object):
    def __init__(self, func):
        super(LogException, self).__init__()
        self.__func = func
        
    def __call__(self, *args, **kwargs):
        try:
            return self.__func(*args, **kwargs)
        except Exception as e:
            error(traceback.format_exc())
            raise e


# proc = pool.apply_async(LogException(func), (ipos,))  
multiprocessing.log_to_stderr()      

def run_as_cmd():
    
    insert = Insert()
    insert.run()

if __name__ == "__main__":

    # procnum = int(sys.argv[1])

    pool = Pool(processes=6)

    for procid in xrange(6):

        # pool.apply_async(run_as_cmd)
        pool.apply_async(LogException(run_as_cmd))

        time.sleep(0.1)
    multiprocessing.log_to_stderr()
    pool.close()
    pool.join()
    