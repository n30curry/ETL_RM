#_*_ coding:utf-8 _*_

# 使用 UTF-8
import sys
reload(sys)
sys.setdefaultencoding("utf-8")

import json

from Lib.pyp import GDB
from Lib.pyp_hybrid.Supervisor import Supervisord

def reorder(tosort_list, order_list):
    """
        重新对序列进行排序

        Input :
            tosort_list : List
                要被重新排列的列表

            order_list : List
                重新排列的顺序
        
        Return : List
            按照 order_list 排列的新序列
    """

    for ipos in xrange(len(order_list)):

        while ipos != order_list[ipos]:

            iorderpos = order_list[ipos]
            tosort_list[ipos], tosort_list[iorderpos] = tosort_list[iorderpos], tosort_list[ipos]
            order_list[ipos], order_list[iorderpos] = order_list[iorderpos], order_list[ipos]

    return tosort_list

class DKGraph(object):
    """
        对图形数据的封装
    """
    def __init__(self, data = None):
        super(DKGraph, self).__init__()
        self.data = data

    def load(self, data):
        self.data = data
        return self

    def _isNotEmpty(self):
        return False if self.data is None else True

    def _isAvailable(self):
        try:
            self.data["nodes"]["keys"]
            self.data["nodes"]["docs"]
            self.data["links"]["keys"]
            self.data["links"]["docs"]
        except KeyError as e:
            return False
        return True

    def isAvailable(self):
        return self._isNotEmpty() and self._isAvailable()

    def clear(self):
        self.data = None
        return self

    def routeQuery(self, starts):
        """
            找到给出点之间的通路
            Input
                starts : List
                    起始点列表

            Return : Dict
                GDB 返回格式的图

            算法描述：
            定义：
                expanded : List
                    已探索节点列表
                selected : List
                    已选择节点列表
                    在列表中的点有到目的地点的通路
                result : Dict
                    结果图

            伪代码:
                将 starts 中的点放入 selected 列表中
                FOR 起始点 IN 起始点列表 THEN
                    对起始点做深度优先搜索，对于每个搜索到的点 A
                    IF A 在 selected 列表中 THEN
                        将 A 放入 result 中
                        将 A 放入 selected 中
                    ENDIF

                    IF A 在 expanded 列表中 THEN
                        不以 A 为起点继续深度优先搜索
                        # 在 expanded 列表中的点都是已经完全搜索过的点
                        # 其所有的边都已经被探索过，如果该点不在 selected 中
                        # 则说明不存在从该点到目标点的路径
                    ENDIF
                
                    IF A 中所有的边都已经探索 THEN
                        将 A 放入 expanded 列表中
                        # 由于是深度优先搜索，因此其字节点应该都已经被探索过
                    ENDIF
                         
                ENDFOR
        """
        
        def _DFS(start, trace):
            """
                深度优先搜索
            """

            for start, end in linkdict.get(start, []):


                # Loop detector
                if trace.count(end) > 0:
                    continue

                trace.append(end)

                if end in selected:
                    result["nodes"]["keys"].extend(trace)

                    linklist = [[trace[i], trace[i+1]] for i in xrange(len(trace) - 1)]
                    result["links"]["keys"].extend(linklist)
                    selected["start"] = ""

                    continue

                if end not in expanded:
                    _DFS(end, list(trace))

                trace.pop()

            expanded["start"] = ""

        def _distinct(graph):
            """
                去重
            """
            graph["nodes"]["keys"] = list(set(graph["nodes"]["keys"]))
            graph["nodes"]["docs"] = []

            links = {"\x00".join(link) : "" for link in graph["links"]["keys"]}
            graph["links"]["keys"] = [key.split("\x00") for key in links.keys()]
            graph["links"]["docs"] = []
            
            return graph    

        result = {
            "nodes" : {"keys" : [], "docs" : []},
            "links" : {"keys" : [], "docs" : []},
        }
        if self.isAvailable() is False:
            return result

        linkdict = {}
        for start, end in self.data["links"]["keys"]:
            linkdict.setdefault(start, []).append((start,end))

        expanded = {}
        selected = {start:"" for start in starts}
        result["nodes"]["keys"] = list(starts)

        for start in starts:
            _DFS(start, [start])

        return _distinct(result)

class PyGDB(Supervisord):
    """ 
        对程老师 GDB 的封装 
        需求 GDB 包
    """
    def __init__(self, loginDict = None, bUsePool = False):
        super(PyGDB, self).__init__()

        # 防止死循环调用 __getattr__
        self.gdb = None

        self.loginDict = loginDict
        self.bUsePool = bUsePool

        self.strUsedTableSet = set()

        self.login(loginDict, bUsePool)

    def __del__(self):
        try:

            for strUsedTable in self.strUsedTableSet:
                self.commit(strUsedTable)
            
            self.logout()
        except Exception as e:
            pass

    def _login(self, loginDict, bUsePool = False):

        if bUsePool is True:
            self.gdb = GDB
            return self

        if loginDict is None:
            return self

        self.gdb = GDB.DB()
        self.gdb.login(loginDict)
        return self

    def _use(self, strTable, bUsePool = False):

        self.gdb.use(strTable)
        self.strUsedTableSet.add(strTable)
        return self

    def login(self, loginDict, bUsePool = False):
        try:
            self._login(loginDict, bUsePool)
        except Exception as e:
            print "[ERROR] GDB -> login"
            raise e

    def use(self, strTable):
        try:
            self._use(strTable)
        except Exception as e:
            print "[ERROR] GDB -> use"
            raise e

    def callback(self, *args, **kwds):
        self.login(self.loginDict, self.bUsePool)

        if self.bUsePool is False:
            for strUsedTable in self.strUsedTableSet:
                self.use(strUsedTable)

        return args, kwds

    # # TODO: 等待 GDB 修复 node 重复的问题，解决后此函数可以删除
    # def query(self, table, query_list, config_dict):
        
    #     graph_dict = self.gdb.query(table, query_list, config_dict)

    #     distinct_dict = {}
    #     distinct_keys = []
    #     distinct_docs = []

    #     graph_nodedocs = graph_dict["nodes"]["docs"]
    #     for ipos, key in enumerate(graph_dict["nodes"]["keys"]):
    #         if key in distinct_dict:
    #             continue

    #         distinct_dict[key] = ""
    #         distinct_keys.append(key)
    #         distinct_docs.append(graph_nodedocs[ipos])
        
    #     graph_dict["nodes"]["keys"] = distinct_keys
    #     graph_dict["nodes"]["docs"] = distinct_docs
    #     return graph_dict

    def queryX(self, strTable, queryData, formatDict):
        data = self.gdb.query(strTable, queryData, formatDict)
        return DKGraph(data)

    def selects(self, str_table, config_dict):
        result = self.gdb.selects(str_table, config_dict)

        reorder(result["nodes"]["docs"], result["nodes"]["rids"])
        reorder(result["links"]["docs"], result["links"]["rids"])

        del result["nodes"]["rids"]
        del result["links"]["rids"]

        return result

    def traverse(self, str_table, position_list = None, int_step = 1000):
        """
            遍历 GDB 中的数据
            Inputs :
                str_table : String
                    GDB 表名

                position_list : List
                    遍历位置信息，需要部分遍历时使用
                    
                    格式为 : [{
                                'sid': 片号, 
                                'count': [(log起始位置, 终止位置), ...], 
                                'xcount': [(dat起始位置, 终止位置), ...],
                            }, ... ]
                    如果为 None，则调用 GDB.shards 遍历全部数据

                int_step : Integer
                    每次请求 GDB 的条目数量
                
                keyonly : Boolean
                    返回数据的格式
                    True : 只返回 key 信息
                    False : 返回 key 和 doc 信息

            Return : List (Generator)
                GDB 数据的 Key
        """

        if position_list is None:
            position_list = []
            for sid in self.gdb.shards(str_table)["list"]:
                docs = self.gdb.keyCount(str_table, sid)
                position_list.append({
                        "sid" : sid,
                        "count" : [(0, docs.get("count", 0))],
                        "xcount" : [(0, docs.get("xcount", 0))],
                    })

        return self.traverseX(str_table, position_list, int_step)

    def traverseX(self, str_table, position_list, int_step = 1000):
        """
            遍历 GDB 中的数据
            Inputs :
                str_table : String
                    GDB 表名

                position_list : List
                    遍历位置信息，需要部分遍历时使用
                    
                    格式为 : [{
                                'sid': 片号, 
                                'count': [(log起始位置, 终止位置), ...], 
                                'xcount': [(dat起始位置, 终止位置), ...],
                            }, ... ]

                int_step : Integer
                    每次请求 GDB 的条目数量
                
                keyonly : Boolean
                    返回数据的格式
                    True : 只返回 key 信息
                    False : 返回 key 和 doc 信息

            Return : List (Generator)
                GDB 数据的 Key
        """
        
        for position_dict in position_list:

            sid = position_dict["sid"]
            for begin, end in position_dict["count"]:
                for pos in xrange(begin, end, int_step):
                    yield self.gdb.logKeys(str_table, sid, pos, int_step)

            for begin, end in position_dict["xcount"]:
                for pos in xrange(begin, end, int_step):
                    yield self.gdb.datKeys(str_table, sid, pos, int_step)

    def traverse_node(self, str_table, position_list = None, int_step = 1000, spliter = "\x00"):
        """
            遍历 GDB 数据中的点
            Inputs :
                str_table : String
                    GDB 表名

                position_list : List
                    遍历位置信息，需要部分遍历时使用
                    
                    格式为 : [{
                                'sid': 片号, 
                                'count': [(log起始位置, 终止位置), ...], 
                                'xcount': [(dat起始位置, 终止位置), ...],
                            }, ... ]
                    如果为 None，则调用 GDB.shards 遍历全部数据

                int_step : Integer
                    每次请求 GDB 的条目数量
                
                keyonly : Boolean
                    返回数据的格式
                    True : 只返回 key 信息
                    False : 返回 key 和 doc 信息

            Return : Dict (Generator)
                GDB 数据的 Key
        """
        
        nodelist = []
        for keylist in self.traverse(str_table, position_list, int_step):
            
            for data in keylist:
                start, end = data.split(spliter)
                if len(end) == 0:
                    nodelist.append(start)

            if len(nodelist) >= int_step:
                yield nodelist[:int_step]
                nodelist = nodelist[int_step:]

        if len(nodelist) != 0:
            yield nodelist
    
    def traverse_link(self, str_table, position_list = None, int_step = 1000, spliter = "\x00"):
        """
            遍历 GDB 数据中的边
            Inputs :
                str_table : String
                    GDB 表名

                position_list : List
                    遍历位置信息，需要部分遍历时使用
                    
                    格式为 : [{
                                'sid': 片号, 
                                'count': [(log起始位置, 终止位置), ...], 
                                'xcount': [(dat起始位置, 终止位置), ...],
                            }, ... ]
                    如果为 None，则调用 GDB.shards 遍历全部数据

                int_step : Integer
                    每次请求 GDB 的条目数量
                
                keyonly : Boolean
                    返回数据的格式
                    True : 只返回 key 信息
                    False : 返回 key 和 doc 信息

            Return : Dict (Generator)
                GDB 数据的 Key
        """

        linklist = []
        for keylist in self.traverse(str_table, position_list, int_step):
            
            for data in keylist:
                linelist = data.split(spliter)
                if len(linelist[1]) != 0:
                    linklist.append(linelist)

            if len(linklist) >= int_step:
                yield linklist[:int_step]
                linklist = linklist[int_step:]

        if len(linklist) != 0:
            yield linklist

    def __getattr__(self, attr):
        res = getattr(self.gdb, attr, None)
        if res is None:
            return super(PyGDB, self).__getattribute__(attr)
        return res
