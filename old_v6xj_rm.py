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

from Lib.pyp_hybrid.PyKDB import PyKDB
from Lib.pyp_hybrid.PyNDB import PyNDB

from Lib.ConfigurationLoadder import ConfigurationLoadder

# KDB_HOST = "127.0.0.1"
KDB_HOST = "192.168.0.237"
KDB_PORT = 9910
KDB_TABLE = "v63xj_kdb_rmlink"

NDB_HOST = "127.0.0.1"
NDB_PORT = 9869
#NDB_TABLE = "v63xj_ndb"
NDB_TABLE = "test"

PROCID = 0
PROCNUM = 1
SERVICE_IDS = None

BATCHNUM = 20

DEFAULT_SETTING = "settings.cfg"

class V6xj_RM(object):
    """docstring for V6xj_RM"""
    def __init__(self):
        super(V6xj_RM, self).__init__()
        self.cfg_loadder = ConfigurationLoadder()
    
    def _kdblogin(self, config):
        
        self.kdb = PyKDB()
        self.kdb.login({"host" : config["host"], "port" : config["port"]})

        self.kdb_table = config["table"]
        self.kdb.use(self.kdb_table)

    def _ndblogin(self, config):
        
        self.ndb = PyNDB()
        self.ndb.login({"host" : config["host"], "port" : config["port"]})

        self.ndb_table = config["table"]
        self.ndb.use(self.ndb_table)

    def load_func(self, module_path, module_name):

        if sys.modules.get(module_path) is None:
            importlib.import_module(module_path)
            
        module = sys.modules[module_path]
        obj = getattr(module, module_name)       
        return obj

    def login(self, kdbconfig, ndbconfig):

        self._kdblogin(kdbconfig)
        self._ndblogin(ndbconfig)

    def load(self, setting, interfere = None):
        
        cfg_dict = self.cfg_loadder.load(setting)
        cfg_dict.update(interfere or {})

        module_path = cfg_dict.get("ModulePath", None)
        self.properties = self.load_func(module_path, cfg_dict["Properties"])

    def isavaliable(self):

        try:
            self.kdb
            self.ndb
        except Exception as e:
            return False

        return True

    def _ranges(self, procid = 0, procnum = 1, serviceids = None):

        ranges = []
        #shards 返回的keys是每個片的最後一個key，list返回的是片號
        for sid in self.kdb.shards(self.kdb_table)["list"]:

            pieceid, serviceid = Num.split(sid)
            if serviceids is not None and serviceid not in serviceids:
                continue

            if pieceid % procnum != procid:
                continue

            docs = self.kdb.keyCount(self.kdb_table, sid)
            ranges.append({
                    "sid" : sid,
                    "count" : [[0, docs.get("count", 0)]],
                    "xcount" : [[0, docs.get("xcount", 0)]],
                })
        # print ranges
        return ranges

    def leftoverids(self, position_list):
        lastids = {}
        for position_dict in position_list:

            sid = position_dict["sid"]
            for begin, end in position_dict["count"]:
                #logKeys 返回的是key的列表
                begins = self.kdb.logKeys(self.kdb_table, sid, begin, 1, 1)
                ends = self.kdb.logKeys(self.kdb_table, sid, end-1, 1, 1)

                if len(begins) > 0:
                    lastids[begins[0].split("\x00")[0]] = ""
                
                if len(ends) > 0:
                    lastids[ends[0].split("\x00")[0]] = ""

            for begin, end in position_dict["xcount"]:
                begins = self.kdb.datKeys(self.kdb_table, sid, begin, 1, 1)
                ends = self.kdb.datKeys(self.kdb_table, sid, end-1, 1, 1)

                if len(begins) > 0:
                    lastids[begins[0].split("\x00")[0]] = ""
                
                if len(ends) > 0:
                    lastids[ends[0].split("\x00")[0]] = ""
        return lastids.keys()

    def update(self, node_dict):

        nodes = []
        nodeposes = [0]
        ipos = itertools.count()
        for node, links in node_dict.items():
            nodes.append(node)
            nodeposes.append(nodeposes[ipos.next()] + len(links))
        docs = self.kdb.selects(self.kdb_table, list(itertools.chain(*node_dict.values())))

        graph_dict = {}
        for ipos, node in enumerate(nodes):

            link_node = map(lambda x: x.split("\x00")[-1], node_dict[node])
            properties = self.properties(docs[nodeposes[ipos]:nodeposes[ipos+1]])
            graph_dict[node] = dict(zip(link_node, properties))
        
        print graph_dict
        exit()
        key1 = []
        key2 = []
        # num = [[],[],[],[]]
        num = []
        for k,v in graph_dict.items():
            for x in v.keys():
                key1.append(k)
                key2.append(x)
                # num[0].append(0)
                # num[1].append(0)
                # num[2].append(0)
                # num[3].append(0)
        print key1,len(key1)
        print key2,len(key2)
        print num
        self.ndb.inserts('test',key1,key2,num)
        self.ndb.dumps('test')
        
    # def update(self, node_dict):

    #     nodes = []
    #     nodeposes = [0]
    #     ipos = itertools.count()
    #     for node, links in node_dict.items():
    #         nodes.append(node)
    #         nodeposes.append(nodeposes[ipos.next()] + len(links))

    #     print node_dict.values()
    #     print node_dict.values()[0]
    #     print node_dict.values()[-1]

    #     """
    #     ['1#13009603502\x001#13109027785', '1#13009609258\x001#18194868517', '1#13009614310\x001#13899326692', '1#13009648157\x001#13199737339', '1#13009644918\x001#13279708291', '1#13009612997\x001#13009672597', '1#13009613828\x001#18997913580', '1#13009633802\x001#18999959144', '1#13009625372\x001#13565821660', '1#13009626833\x001#13095185767', '1#13009634188\x001#13209908266', '1#13009611671\x001#15894029176', '1#13009647453\x001#13201299821', '1#13009627202\x001#13319852327', '1#13009619675\x001#15199128597', '1#13009603088\x001#15001659863', '1#13009629087\x001#13179832546', '1#13009604409\x001#13565333351', '1#13009611413\x001#13669905543', '1#13009610005\x001#15299757581', '1#13009609910\x001#13279948988']
    #     """

    #     print self.kdb.kdb.selects(self.kdb_table, ['1#13009603502\x001#13109027785', '1#13009609258\x001#18194868517'])
    #     # print self.kdb.kdb.selects(self.kdb_table, node_dict.values()[-1])

    #     docs = self.kdb.kdb.selects(self.kdb_table, list(itertools.chain(*node_dict.values())))

    #     print list(itertools.chain(*node_dict.values()))
    #     print docs
    #     print "======================"

    #     # for ipos, node in enumerate(nodes):
    #     #     print nodeposes[ipos+1], len(docs)
    #     #     print node, docs[nodeposes[ipos]:nodeposes[ipos+1]]

    #     exit()

    def last_turn(self, procid, procnum, serviceids, checkpoint):
        
        assert self.isavaliable(), "kdb or ndb not ready"

        node_dict = {}

        ranges = self._ranges()
        nodes = self.leftoverids(ranges)

        links = self.kdb.query(self.kdb_table, nodes)["keys"]
        for link in links:
            startnode = link.split("\x00")[0]
            node_dict.setdefault(startnode, []).append(link)
        
        self.update(node_dict)

    def run(self, procid, procnum, serviceids, checkpoint):

        assert self.isavaliable(), "kdb or ndb not ready"

        current = None
        counter = itertools.count()
        node_dict = {}

        ranges = self._ranges(procid, procnum, serviceids)
        # for keylist in self.kdb.traverse(self.kdb_table, ranges, check_path = checkpoint):
        for links in self.kdb.traverse(self.kdb_table, ranges):

            for link in links:

                startnode = link.split("\x00")[0]

                if current == startnode:
                    node_dict[startnode].append(link)
                    counter.next()
                    continue

                if counter.next() > BATCHNUM:
                    self.update(node_dict)
                    node_dict = {}
                    counter = itertools.count()

                current = startnode
                node_dict.setdefault(startnode, []).append(link)
        self.update(node_dict)


class Mode:
    DISTRIBUTE = 0
    SINGLE = 1
    PROFILE = 2
    LEFTOVER = 3

def run_as_cmd(procid = 0, procnum = 1, serviceids = None, checkpoint = None,
                kdbhost = None, kdbport = None, kdbtable = None,
                ndbhost = None, ndbport = None, ndbtable = None):

    str_args = ""
    str_args += " --procid={0} ".format(procid)
    str_args += " --procnum={0} ".format(procnum)
    str_args += "" if serviceids is None else " --serviceids={0} ".format(serviceids)
    str_args += "" if checkpoint is None else " --checkpoint={0} ".format(checkpoint)

    str_args += "" if kdbhost is None else " --kdbhost={0} ".format(kdbhost)
    str_args += "" if kdbport is None else " --kdbport={0} ".format(kdbport)
    str_args += "" if kdbtable is None else " --kdbtable={0} ".format(kdbtable)

    str_args += "" if ndbhost is None else " --ndbhost={0} ".format(ndbhost)
    str_args += "" if ndbport is None else " --ndbport={0} ".format(ndbport)
    str_args += "" if ndbtable is None else " --ndbtable={0} ".format(ndbtable)

    os.system("python {program} -s {args}".format(
            program = sys.argv[0],
            args = str_args,
        ))

def run_as_distribute(arguments):

    procid = arguments["procid"]
    procnum = arguments["procnum"]
    serviceids = arguments["serviceids"]
    checkpoint = arguments["checkpoint"]

    kdbhost = arguments["kdbhost"]
    kdbport = arguments["kdbport"]
    kdbtable = arguments["kdbtable"]

    ndbhost = arguments["ndbhost"]
    ndbport = arguments["ndbport"]
    ndbtable = arguments["ndbtable"]

    pool = Pool(processes = procnum)
    for procid in xrange(procnum):
        pool.apply_async(run_as_cmd, (procid, procnum, serviceids, procid,
                                        kdbhost, kdbport, kdbtable,
                                        ndbhost, ndbport, ndbtable,))
        time.sleep(0.1)

    pool.close()
    pool.join()

def run_as_leftover(arguments):

    procid = arguments["procid"]
    procnum = arguments["procnum"]
    serviceids = arguments["serviceids"]
    checkpoint = arguments["checkpoint"]
    setting = arguments["setting"]

    kdbconfig = {
        "host" : arguments["kdbhost"],
        "port" : arguments["kdbport"],
        "table" : arguments["kdbtable"],
    }

    ndbconfig = {
        "host" : arguments["ndbhost"],
        "port" : arguments["ndbport"],
        "table" : arguments["ndbtable"],
    }

    handler = V6xj_RM()
    handler.login(kdbconfig, ndbconfig)
    handler.load(setting)
    handler.last_turn(procid, procnum, serviceids, checkpoint)

def run_as_single(arguments):

    procid = arguments["procid"]
    procnum = arguments["procnum"]
    serviceids = arguments["serviceids"]
    checkpoint = arguments["checkpoint"]
    setting = arguments["setting"]

    kdbconfig = {
        "host" : arguments["kdbhost"],
        "port" : arguments["kdbport"],
        "table" : arguments["kdbtable"],
    }

    ndbconfig = {
        "host" : arguments["ndbhost"],
        "port" : arguments["ndbport"],
        "table" : arguments["ndbtable"],
    }

    handler = V6xj_RM()
    handler.login(kdbconfig, ndbconfig)
    handler.load(setting)
    handler.run(procid, procnum, serviceids, checkpoint)

def run_as_profile(arguments):
    
    import pstats    
    import cProfile

    procid = arguments["procid"]
    procnum = arguments["procnum"]
    serviceids = arguments["serviceids"]
    checkpoint = arguments["checkpoint"]
    setting = arguments["setting"]

    kdbconfig = {
        "host" : arguments["kdbhost"],
        "port" : arguments["kdbport"],
        "table" : arguments["kdbtable"],
    }

    ndbconfig = {
        "host" : arguments["ndbhost"],
        "port" : arguments["ndbport"],
        "table" : arguments["ndbtable"],
    }

    handler = V6xj_RM()
    handler.login(kdbconfig, ndbconfig)
    handler.load(setting)

    cmd = "handler.run({procid}, {procnum}, {serviceids}, {checkpoint})".format(
            procid = procid, procnum = procnum, 
            serviceids = serviceids, checkpoint = checkpoint
        )

    cProfile.run(cmd, filename="result.out", sort="tottime")
    p = pstats.Stats("result.out")
    p.strip_dirs().sort_stats("tottime", "name").print_stats(0.5)

mode_dict = {
    Mode.DISTRIBUTE : run_as_distribute,
    Mode.SINGLE : run_as_single,
    Mode.PROFILE : run_as_profile,
    Mode.LEFTOVER : run_as_leftover,
}

import time
import getopt
if __name__ == "__main__":
    begin = time.time()

    optcmds = [
        "procid=", 
        "procnum=", 
        "serviceids=", 
        "checkpoint=",

        "kdbhost=", 
        "kdbport=", 
        "kdbtable=", 

        "ndbhost=", 
        "ndbport=",
        "ndbtable=",

        "setting=",
    ]
    opts, args = getopt.getopt(sys.argv[1:], "dspl", optcmds)

    mode = None
    arguments = {
        "procid" : 0,
        "procnum" : 1,
        "serviceids" : None,
        "checkpoint" : "0",

        "kdbhost" : KDB_HOST,
        "kdbport" : KDB_PORT,
        "kdbtable" : KDB_TABLE,

        "ndbhost" : NDB_HOST,
        "ndbport" : NDB_PORT,
        "ndbtable" : NDB_TABLE,

        "setting" : DEFAULT_SETTING,
    }

    for opt, arg in opts:

        if opt in ("-d"):
            mode = Mode.DISTRIBUTE
        elif opt in ("-s"):
            mode = Mode.SINGLE
        elif opt in ("-p"):
            mode = Mode.PROFILE
        elif opt in ("-l"):
            mode = Mode.LEFTOVER

        elif opt in ("--kdbhost",):
            arguments["kdbhost"] = arg
        elif opt in ("--kdbport",):
            arguments["kdbport"] = int(arg)
        elif opt in ("--kdbtable",):
            arguments["kdbtalbe"] = arg

        elif opt in ("--ndbhost",):
            arguments["ndbhost"] = arg
        elif opt in ("--ndbport",):
            arguments["ndbport"] = int(arg)
        elif opt in ("--ndbtable",):
            arguments["ndbtalbe"] = arg

        elif opt in ("--procid",):
            arguments["procid"] = int(arg)
        elif opt in ("--procnum",):
            arguments["procnum"] = int(arg)
        elif opt in ("--serviceids",):
            arguments["serviceids"] = eval(arg)
        elif opt in ("--checkpoint",):
            arguments["checkpoint"] = arg

        elif opt in ("--setting",):
            arguments["setting"] = arg

    mode_dict[mode](arguments)

    print "[Finished] Using: {0}s".format(time.time() - begin)

