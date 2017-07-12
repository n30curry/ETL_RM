#_*_ coding:utf-8 _*_

# 使用 UTF-8
import sys
reload(sys)
sys.setdefaultencoding("utf-8")

from Lib.pyp import FDB
from Lib.pyp_hybrid.Supervisor import Supervisord

class _Iterator(object):
    
    def __init__(self):
        super(_Iterator, self).__init__()

    def top(self):
        """
            返回顶部 ID
        """
        return None

    def pop(self):
        """
            弹出顶部 ID
            返回值为新的顶部 ID
        """
        return None

    def set_state(self, state):
        pass

    def state(self):
        """
            返回断点信息
        """
        return ([], 1)

    def __iter__(self):
        return self

    def next(self):
        
        value = self.top()
        if value is None:
            raise StopIteration()

        self.pop()
        return value

class _RsNextIterator(_Iterator):
    """
        对由 rsNext 函数的返回值进行封装，用于多路归并
            
        Input:
            handler : Object
                执行 rsNext 的句柄

            table : String
                使用的表名称

            key : String
                查询的关键词

            count : Integer
                每次查询数据库条目数量

            check : List
                断点信息

            num : Integer
                与断点信息配合，记录使用数据个数
    """
    def __init__(self, handler, table, key, count = 1000, check = None, num = None):
        super(_RsNextIterator, self).__init__()

        self.handler = handler
        self.table = table
        self.key = key
        self.count = count

        self.set_state((check or [], num or 0),)

    def top(self):
        """
            返回顶部 ID
        """
        return self.topid

    def pop(self):
        """
            弹出顶部 ID
            返回值为新的顶部 ID
        """
        try:
            self.topid = self.iter.next()
        except StopIteration as e:
            self.topid = None

        return self.topid

    def state(self):
        return (self.lastcheck, self.num)

    def set_state(self, state):
        
        # 记录可获取当前数据的断点
        self.lastcheck = []
        # 记录当前数据使用个数
        self.num = 0

        check, num = state

        self.iter = self._rsNext(self.handler, self.table, self.key, self.count, check)
        
        for _ in xrange(num or 1):
            self.pop()

    def _rsNext(self, handler, table, key, count = 1000, check = None):
        """
            调用 rsNext 函数，接管 check 断点
            如果剩余队列为空，则返回 None

            Input:
                handler : Object
                    执行 rsNext 的句柄

                table : String
                    使用的表名称

                key : String
                    查询的关键词

                count : Integer
                    每次查询数据库条目数量

                check : List
                    断点信息

        """

        check = check or []
        while True:

            # 记录可获取当前数据的断点
            self.lastcheck = check
            self.num = 0
            matched_dict = handler.rsNext(table, key, {"count" : count, "check" : check})

            matched_list = matched_dict["list"]
            check = matched_dict["check"]

            if len(matched_list) == 0:
                break

            for longid in matched_list:
                self.num += 1
                yield longid   

class _LogicAND(_Iterator):
    """docstring for _LogicAND"""
    def __init__(self, *args, **kwds):
        super(_LogicAND, self).__init__()

        state = kwds.get("state", None)
        self.args = list(args)
        self.kwds = dict(kwds)

        self.set_state(state)

    def top(self):
        """
            返回顶部 ID
        """
        return self.topid

    def pop(self):
        """
            弹出顶部 ID
            返回值为新的顶部 ID
        """
        try:
            self.topid = self.iter.next()
        except StopIteration as e:
            self.topid = None

        return self.topid
    
    def set_state(self, state_list):

        if state_list is not None:

            if len(state_list) != len(self.args):
                return False

            for ipos, state in enumerate(state_list):
                self.args[ipos].set_state(state)

        self.iter = self.run(*list(self.args), **dict(self.kwds))
        self.pop()

        return True

    def state(self):

        state_list = []
        for obj in self.args:
            state_list.append(obj.state())

        return state_list

    def _isinstance(type_tuple):
        
        def deco(func):

            def inner(self, *args, **kwds):

                for obj in args:
                    if isinstance(obj, type_tuple) is False:
                        return None

                return func(self, *args, **kwds)
        
            return inner
        return deco

    @_isinstance((_Iterator,),)
    def run(self, *args, **kwds):

        args = list(args)
        ilength = len(args)
        count = kwds.get("count", -1)

        iresult_count = 0
        while True:

            if (iresult_count == count) or (len(args) != ilength):
                break

            imax = -1
            imax_count = 0
            imaxpos = []
            idelpos = []
            for ipos in xrange(len(args)-1, -1, -1):
                
                value = args[ipos].top()
                if value is None:
                    # 记录要删除的位置，此处不删除是怕破坏记录的数组位置
                    idelpos.append(ipos)
                    continue

                # 找到最大值，并且重新统计相同最大值列表
                if value > imax:
                    imax = value
                    imaxpos = [ipos]
                    imax_count += 1
                elif value == imax:
                    # 在相同最大值列表中记录最大值位置
                    imaxpos.append(ipos)
                    imax_count += 1

            if imax_count == ilength:
                yield imax
                iresult_count += 1

            for ipos in imaxpos:
                args[ipos].pop()

            # 由于 idelpos 是通过倒序遍历添加的数组
            # 因此其存放的位置是从大到小依次排列，可以直接遍历删除(相当于倒序删除)
            for ipos in idelpos:
                del args[ipos]

class _LogicOR(_Iterator):
    """docstring for _LogicOR"""
    def __init__(self, *args, **kwds):
        super(_LogicOR, self).__init__()

        state = kwds.get("state", None)
        self.args = list(args)
        self.kwds = dict(kwds)

        self.set_state(state)

    def top(self):
        """
            返回顶部 ID
        """
        return self.topid

    def pop(self):
        """
            弹出顶部 ID
            返回值为新的顶部 ID
        """
        try:
            self.topid = self.iter.next()
        except StopIteration as e:
            self.topid = None

        return self.topid
    
    def set_state(self, state_list):

        if state_list is not None:

            if len(state_list) != len(self.args):
                return False

            for ipos, state in enumerate(state_list):
                self.args[ipos].set_state(state)

        self.iter = self.run(*list(self.args), **dict(self.kwds))
        self.pop()

        return True

    def state(self):

        state_list = []
        for obj in self.args:
            state_list.append(obj.state())

        return state_list

    def _isinstance(type_tuple):
        
        def deco(func):

            def inner(self, *args, **kwds):

                for obj in args:
                    if isinstance(obj, type_tuple) is False:
                        return None

                return func(self, *args, **kwds)
        
            return inner
        return deco

    @_isinstance((_Iterator,),)
    def run(self, *args, **kwds):

        args = list(args)
        num = kwds.get("num", -1)

        icount = 0
        while True:

            if (icount == num) or (len(args) == 0):
                break

            imax = -1
            imaxpos = -1
            idelpos = []
            for ipos in xrange(len(args)-1, -1, -1):

                value = args[ipos].top()
                if value is None:
                    # 记录要删除的位置，此处不删除是怕破坏记录的数组位置
                    idelpos.append(ipos)
                    continue

                # 找出并且记录最大值
                if value > imax:
                    imax = value
                    imaxpos = ipos
                elif value == imax:
                    # 消除重复
                    args[ipos].pop()

            if imax > 0:
                yield imax
                icount += 1

            if imaxpos >= 0:
                args[imaxpos].pop()

            # 由于 idelpos 是通过倒序遍历添加的数组
            # 因此其存放的位置是从大到小依次排列，可以直接遍历删除(相当于倒序删除)
            for ipos in idelpos:
                del args[ipos]

class PyFDB(Supervisord):
    """ 
        对程老师 FDB 的封装 
        需求 FDB 包
    """
    def __init__(self, loginDict = None, bUsePool = False, *args, **kwds):
        super(PyFDB, self).__init__(*args, **kwds)

        # 防止死循环调用 __getattr__
        self.fdb = None

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
            self.fdb = FDB
            return self

        if loginDict is None:
            return self

        self.fdb = FDB.DB()
        self.fdb.login(loginDict)
        return self

    def _use(self, strTable):

        self.fdb.use(strTable)
        self.strUsedTableSet.add(strTable)
        return self

    def login(self, loginDict, bUsePool = False):
        try:
            self._login(loginDict, bUsePool)
        except Exception as e:
            print "[ERROR] FDB -> login"
            raise e

    def use(self, strTable):
        try:
            self._use(strTable)
        except Exception as e:
            print "[ERROR] FDB -> use"
            raise e

    def callback(self, *args, **kwds):

        self.login(self.loginDict, self.bUsePool)

        if self.bUsePool is False:
            for strUsedTable in self.strUsedTableSet:
                self.use(strUsedTable)

        return args, kwds

    def gen_search(self, table, strkey, ibegin = -1, inum = 100):
        
        doc = self.termFreq(table, strkey)
        config = {
            "freq" : doc["freq"],
            "locat" : doc["locat"],
            "begin" : ibegin,
            "count" : inum,
        }

        while True:

            fdb_matched_dict = self.fdb.rsNext(table, doc["term"], config)

            if len(fdb_matched_dict["docids"]) == 0:
                break

            inum = yield fdb_matched_dict
            if inum is not None:
                config["count"] = inum
                
            config["begin"] = fdb_matched_dict["docids"][-1]-1

    def rsNext(self, table, term, config):

        doc = self.fdb.rsNext(table, term, config)

        mdbid_rank_dict = dict(zip(doc["docids"], doc["rank"]))

        mdbid_rank_list = sorted(mdbid_rank_dict.items(), key=lambda x: x[1], reverse=False)

        return map(lambda x: x[0], mdbid_rank_list)

    def termFreq(self, table, query):

        doc = self.fdb.termFreq(table, query)

        doc_list = []
        for ipos, freq in enumerate(doc["freq"]):
            doc_list.append([freq, doc["term"][ipos], doc["locat"][ipos]])

        reorder_dict = {
            "freq" : [],
            "term" : [],
            "locat" : []
        }
        for freq, term, locat in sorted(doc_list, key=lambda x: x[0]):
            if freq == 0:
                continue

            reorder_dict["freq"].append(freq)
            reorder_dict["term"].append(term)
            reorder_dict["locat"].append(locat)

        return reorder_dict

    def rsNextIter(self, table, key, count = 1000, check = None, num = None):
        return _RsNextIterator(self.fdb, table, key, count, check, num)

    def rsAndIter(self, *args, **kwds):
        return _LogicAND(*args, **kwds)

    def rsOrIter(self, *args, **kwds):
        return _LogicOR(*args, **kwds)

    def __getattr__(self, attr):
        res = getattr(self.fdb, attr, None)
        if res is None:
            return super(PyFDB, self).__getattribute__(attr)
        return res

