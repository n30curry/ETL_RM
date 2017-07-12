#_*_ coding:utf-8 _*_

# 使用 UTF-8
import sys
reload(sys)
sys.setdefaultencoding("utf-8")

from Lib.pyp import BDB
from Lib.pyp_hybrid.Supervisor import Supervisord

class PyBDB(Supervisord):
    """ 
        对程老师 BDB 的封装 
        需求 BDB 包
    """
    def __init__(self, loginDict = None, bUsePool = False, *args, **kwds):
        super(PyBDB, self).__init__(*args, **kwds)

        # 防止死循环调用 __getattr__
        self.bdb = None
        
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
            self.bdb = BDB
            return self

        if loginDict is None:
            return self

        self.bdb = BDB.DB()
        self.bdb.login(loginDict)
        return self

    def login(self, loginDict, bUsePool = False):

        try:
            self._login(loginDict, bUsePool)
        except Exception as e:
            print "[ERROR] BDB -> login"
            raise e

    def _use(self, strTable):

        self.bdb.use(strTable)
        self.strUsedTableSet.add(strTable)
        return self

    def use(self, strTable):
        try:
            self._use(strTable)
        except Exception as e:
            print "[ERROR] BDB -> use"
            raise e

    def callback(self, *args, **kwds):

        self.login(self.loginDict, self.bUsePool)

        if self.bUsePool is False:
            for strUsedTable in self.strUsedTableSet:
                self.use(strUsedTable)

        return args, kwds

    def queryX(self, strTable, queryData, formatDict):
        data = self.bdb.query(strTable, queryData, formatDict)

        queryDict = {}
        for strKey, strDoc in zip(data["keys"], data["docs"]):
            queryDict.setdefault(strKey, []).append(strDoc)

        return queryDict


    def traverse(self, str_table, position_list = None, int_step = 1000):
        """
            遍历 BDB 中的数据
            Inputs :
                str_table : String
                    BDB 表名

                position_list : List
                    遍历位置信息，需要部分遍历时使用
                    
                    格式为 : [{
                                'sid': 片号, 
                                'count': [(log起始位置, 终止位置), ...], 
                                'xcount': [(dat起始位置, 终止位置), ...],
                            }, ... ]
                    如果为 None，则调用 BDB.shards 遍历全部数据

                int_step : Integer
                    每次请求 BDB 的条目数量
                
                keyonly : Boolean
                    返回数据的格式
                    True : 只返回 key 信息
                    False : 返回 key 和 doc 信息

            Return : List (Generator)
                BDB 数据的 Key
        """

        if position_list is None:
            position_list = []
            for sid in self.bdb.shards(str_table)["list"]:
                docs = self.bdb.keyCount(str_table, sid)
                position_list.append({
                        "sid" : sid,
                        "count" : [(0, docs.get("count", 0))],
                        "xcount" : [(0, docs.get("xcount", 0))],
                    })

        return self.traverseX(str_table, position_list, int_step)

    def traverseX(self, str_table, position_list, int_step = 1000):
        """
            遍历 BDB 中的数据
            Inputs :
                str_table : String
                    BDB 表名

                position_list : List
                    遍历位置信息，需要部分遍历时使用
                    
                    格式为 : [{
                                'sid': 片号, 
                                'count': [(log起始位置, 终止位置), ...], 
                                'xcount': [(dat起始位置, 终止位置), ...],
                            }, ... ]

                int_step : Integer
                    每次请求 BDB 的条目数量
                
                keyonly : Boolean
                    返回数据的格式
                    True : 只返回 key 信息
                    False : 返回 key 和 doc 信息

            Return : List (Generator)
                BDB 数据的 Key
        """
        
        for position_dict in position_list:

            sid = position_dict["sid"]
            for begin, end in position_dict["count"]:
                for pos in xrange(begin, end, int_step):
                    yield self.bdb.logKeys(str_table, sid, pos, int_step)

            for begin, end in position_dict["xcount"]:
                for pos in xrange(begin, end, int_step):
                    yield self.bdb.datKeys(str_table, sid, pos, int_step)

    def __getattr__(self, attr):
        res = getattr(self.bdb, attr, None)
        if res is None:
            return super(PyBDB, self).__getattribute__(attr)
        return res
