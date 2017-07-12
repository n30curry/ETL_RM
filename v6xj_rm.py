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
from Lib.pyp import Util

from Lib.pyp_hybrid.PyKDB import PyKDB
# from Lib.pyp_hybrid.PyNDB import PyNDB
from Lib.pyp import NDB

from Lib.ConfigurationLoadder import ConfigurationLoadder
import xml.etree.ElementTree as Etreer
from Lib.pyp import Num
import math

# KDB_HOST = "localhost"
KDB_PORT = 9910
KDB_HOST = "192.168.0.208"
# KDB_PORT = 19910
# KDB_TABLE = "v63xj_kdb_rmlink"
KDB_TABLE = 'kdb_rmlink'

NDB_HOST = "192.168.0.207"
# NDB_HOST = "localhost"
NDB_PORT = 9869
NDB_TABLE = "test"

PROCID = 0
PROCNUM = 1
SERVICE_IDS = None


BATCHNUM = 10000

DEFAULT_SETTING = "settings.cfg"

class V6xj_RM(object):
    """docstring for V6xj_RM"""
    def __init__(self):
        super(V6xj_RM, self).__init__()
        self.a = 0
        self.cfg_loadder = ConfigurationLoadder()

        tree = Etreer.parse('data52.xml')
        root = tree.getroot()
        tag = root.tag

        # self.pattern = {}
        # self.nodedict = {}
        # for scene in root.findall('scene'):
        #     sid = scene.get('id')
        #     weight = scene.get('weight')
        #     self.pattern[sid] = {}

        #     self.pattern[sid]['weight'] = weight
        #     #name = json.dumps(scene.get('name'),ensure_ascii=False)
        #     #print sid,name
        #     for ty in scene.findall('type'):
        #         typeid = ty.get('id')
        #         self.pattern[sid][typeid] = []

        #         for link in ty.findall('link'):
        #             start = link.get('start')
        #             end = link.get('end')
        #             o0 = link.get('o0')
        #             d0 = link.get('d0')
        #             weight = link.get('weight')
        #             self.pattern[sid][typeid].append({'start':start,'end':end, 'o0':o0, 'd0':d0, 'weight':weight})

        # for node in root.findall('node'):
        #     sid = node.get('id')
        #     c0 = node.get('c0')
        #     l0 = node.get('l0')
        #     self.nodedict[sid] = [c0,l0]

        self.pattern = {}
        for scene in root.findall('scene'):
            d = {}
            weight = float(scene.get('weight'))
            sid = int(scene.get('id'))
            o0 = float(scene.get('o0'))
            d['weight'] = weight
            d['o0'] = o0
            self.pattern[sid] = d

        self.role_dict = {}
        for role in root.findall('role'):
            weight = float(role.get('weight'))
            sid = int(role.get('id'))
            self.role_dict[sid] = weight

        self.nodedict = {}
        for node in root.findall('node'):
            sid = node.get('id')
            c0 = int(node.get('c0'))
            self.nodedict[sid] = c0

    def _kdblogin(self, config):
        
        self.kdb = PyKDB()
        self.kdb.login({"host" : config["host"], "port" : config["port"], 'timeout':600})

        self.kdb_table = config["table"]
        self.kdb.use(self.kdb_table)

    def _ndblogin(self, config):
        
        # self.ndb = PyNDB()
        # self.ndb.login({"host" : config["host"], "port" : config["port"]})

        # self.ndb_table = config["table"]
        # self.ndb.use(self.ndb_table)
        self.ndb = NDB.DB()
        self.ndb.login({'host':NDB_HOST,'port':NDB_PORT,'timeout':600})
        self.ndb.use(NDB_TABLE)
        
    def load_func(self, module_path, module_name):

        if sys.modules.get(module_path) is None:
            importlib.import_module(module_path)
            
        module = sys.modules[module_path]
        obj = getattr(module, module_name)       
        return obj

    def login(self, kdbconfig, ndbconfig):

        print "kdb login ..."
        self._kdblogin(kdbconfig)
        print "kdb login success"

        print "ndb login ..."
        self._ndblogin(ndbconfig)
        print "ndb login success"

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

        return ranges

    def leftoverids(self, position_list):

        lastids = {}
        for position_dict in position_list:

            sid = position_dict["sid"]
            for begin, end in position_dict["count"]:
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

        try:
            docs = self.kdb.selects(self.kdb_table, list(itertools.chain(*node_dict.values())))
        except Exception as e:
            print "kdb error"
            
        else:

            graph_dict = {}
            for ipos, node in enumerate(nodes):

                link_node = map(lambda x: x.split("\x00")[-1], node_dict[node])
                properties = self.properties(docs[nodeposes[ipos]:nodeposes[ipos+1]])
                # import time
                # print properties 
                graph_dict[node] = dict(zip(link_node, properties))

            # print graph_dict
            # exit()
            # print self.pattern 
            # exit()

            key_start,key_end,keys,nums = [],[],[],[]

            for startnode,endpos in graph_dict.items():

                keys.append(startnode)
                key_start.append(startnode)
                end_list, num1 = [], []

                for endnode,endscene in endpos.items():

                    keys.append(endnode)
                    end_list.append(endnode)

                    Flag1 = True
                    Flag2 = True
                    Flag3 = True
                    start = startnode.split('#')[0]
                    end = endnode.split('#')[0]

                    if start == '1' and end == '12':
                        rank = 501
                        num1.append(rank)
                        continue
                    elif start == '12' and end == '4':
                        rank = 501
                        num1.append(rank)
                        continue
                    elif start == '4' and end == '3':
                        rank = 501
                        num1.append(rank)
                        continue
                    elif start == '12' and end == '1':
                        rank = 501
                        num1.append(rank)
                        continue
                    elif start == '4' and end == '12':
                        rank = 501
                        num1.append(rank)
                        continue
                    elif start == '3' and end == '4':
                        rank = 501
                        num1.append(rank)
                        continue

                    sn = 0 #sn是夸场景num的总数
                    for scene in endscene:
                        sn += scene[2]

                    
                    if sn < 6:
                        num = sn
                        Flag2 = False
                    elif sn > 105 and sn < 501:
                        num = sn
                        Flag2 = False
                    elif sn > 500:
                        num = sn
                        Flag2 = False

                    count = 0
                    for scene in endscene:

                        if scene[0] in [111,112,113] and scene[1] in [3,4,5]:
                            Flag3 = False
                        elif scene[0] in [311,312,321,322,331,332,333,334,335,336,341,342,343,351,352,353, 354, 355, 356,331,332,333,334,335,336] and (start== end or (start in ['1','2'] and end in ['1','2'])) and scene[1] not in [3,4,5]:
                            Flag1 = False
                        try:
                            n = self.pattern[scene[0]]['weight'] * self.pattern[scene[0]]['o0'] *self.role_dict[scene[1]] * scene[2]
                            count += n
                        except:
                            count = 25


                    d = 100
                    Omax = 45
                    s = float(Omax) / d
                    a = count / s
                    if a < d:
                        num_count = a + 1 + 5
                    else:
                        num_count = a + 5

                    if Flag3 == False:
                        rank = 501
                        # 没有O值
                    else:
                        if Flag1 == False:
                            rank = 0
                            # 没有O值
                        else:
                            if Flag2 == False:
                                rank = num
                                # 没有O值
                            else:
                                # rank = 0
                                # O = count
                                rank = num_count


                    num1.append(rank)

                key_end.append(end_list)
                nums.append(num1)

            ranks = map(lambda x:self.nodedict[x], map(lambda x:x.split('#')[0], keys))
            rank_start = map(lambda x:self.nodedict[x], map(lambda x:x.split('#')[0], key_start))
            try:
                self.ndb.inserts(NDB_TABLE, keys, ranks)
            except Exception as e:
                # print "inserts ndbtable{0}, keys{1}, ranks{2}".format(NDB_TABLE,keys,ranks)
                print "ndb insert Error"
                # pass
            else:
                key_start = map(lambda x:Util.md5(x), key_start)
                rank_end = map(lambda x: map(lambda y:self.nodedict[y], map(lambda z:z.split('#')[0], x)), key_end)
                # self.ndb.insertlink(NDB_TABLE, key_start, key_end, nums)
                key_end = map(lambda x: map(lambda y:Util.md5(y), x), key_end)
                # self.ndb.inserts(NDB_TABLE, key_start, rank_start, key_end, rank_end, nums)
                try:
                    self.ndb.insertsX(NDB_TABLE, key_start, key_end, nums)
                    # print "success"
                    # raise TypeError
                except Exception as e:
                    # print "insertsX ndbtable{0}, key_start{1}, key_end{2}, nums{3}".format(NDB_TABLE,key_start, key_end, nums)
                    print "ndb insertsX Error"
                    # pass
                    # print "time"



    def last_turn(self):
        
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

        # print procid, procnum, serviceids, checkpoint, ranges
        # return None

        for links in self.kdb.traverse(self.kdb_table, ranges, check_path = checkpoint):
        # for links in self.kdb.traverse(self.kdb_table, ranges):
            # print "******"
            # print len(links)
            # print "******"
            # self.a += len(links)
            # print self.a
            # try:
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
            # except Exception as e:
            #     print "links",links
            #     raise e
        # print "*******************"
        # print node_dict
        # print "*******************"
        # exit()
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

    if checkpoint is None:
        checkpoint = "{0}.checkpoint".format(procid)
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

    startid = arguments["startid"]
    endid = arguments["endid"]
    procnum = arguments["procnum"]
    serviceids = arguments["serviceids"]
    checkpoint = arguments["checkpoint"]

    kdbhost = arguments["kdbhost"]
    kdbport = arguments["kdbport"]
    kdbtable = arguments["kdbtable"]

    ndbhost = arguments["ndbhost"]
    ndbport = arguments["ndbport"]
    ndbtable = arguments["ndbtable"]

    pool = Pool(processes = endid - startid)
    for procid in xrange(startid, endid):
        pool.apply_async(run_as_cmd, (procid, procnum, serviceids, procid,
                                        kdbhost, kdbport, kdbtable,
                                        ndbhost, ndbport, ndbtable,))
        time.sleep(0.1)

    pool.close()
    pool.join()

def run_as_leftover(arguments):

    startid = arguments["startid"]
    endid = arguments["endid"]
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
    handler.last_turn()

def run_as_single(arguments):

    startid = arguments["startid"]
    endid = arguments["endid"]
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

    startid = arguments["startid"]
    endid = arguments["endid"]
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
        "startid=", 
        "endid=", 
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
        "startid" : 0,
        "endid" : 1,
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

        elif opt in ("--startid",):
            arguments["startid"] = int(arg)
        elif opt in ("--endid",):
            arguments["endid"] = int(arg)
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

