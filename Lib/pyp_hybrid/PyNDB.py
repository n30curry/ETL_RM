#_*_ coding:utf-8 _*_

# 使用 UTF-8
import sys
reload(sys)
sys.setdefaultencoding("utf-8")

from Lib.pyp import NDB
from Lib.pyp_hybrid.Supervisor import Supervisord

class PyNDB(Supervisord):
    """ 
        对程老师 NDB 的封装 
        需求 NDB 包
    """
    def __init__(self, loginDict = None, bUsePool = False, *args, **kwds):
        super(PyNDB, self).__init__(*args, **kwds)

        # 防止死循环调用 __getattr__
        self.ndb = None
        
        self.loginDict = loginDict
        self.bUsePool = bUsePool

        self.strUsedTableSet = set()

        self.login(loginDict, bUsePool)

    def __del__(self):
        try:
            self.logout()
        except Exception as e:
            pass

    def _login(self, loginDict, bUsePool = False):

        if bUsePool is True:
            self.ndb = NDB
            return self

        if loginDict is None:
            return self

        self.ndb = NDB.DB()
        self.ndb.login(loginDict)
        return self

    def _use(self, strTable):

        self.ndb.use(strTable)
        self.strUsedTableSet.add(strTable)
        return self

    def login(self, loginDict, bUsePool = False):
        try:
            self._login(loginDict, bUsePool)
        except Exception as e:
            print "[ERROR] NDB -> login"
            raise e

    def use(self, strTable):
        try:
            self._use(strTable)
        except Exception as e:
            print "[ERROR] NDB -> use"
            raise e

    def callback(self, *args, **kwds):

        self.login(self.loginDict, self.bUsePool)

        if self.bUsePool is False:
            for strUsedTable in self.strUsedTableSet:
                self.use(strUsedTable)

        return args, kwds

    def __getattr__(self, attr):
        res = getattr(self.ndb, attr, None)
        if res is None:
            return super(PyNDB, self).__getattribute__(attr)
        return res
