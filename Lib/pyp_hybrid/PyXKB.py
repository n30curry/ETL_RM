#_*_ coding:utf-8 _*_

# 使用 UTF-8
import sys
reload(sys)
sys.setdefaultencoding("utf-8")

from Lib.pyp import XKB
from Lib.pyp_hybrid.Supervisor import Supervisord

class PyXKB(Supervisord):
    """ 
        对程老师 XKB 的封装 
        需求 XKB 包
    """
    def __init__(self, loginDict = None, bUsePool = False, *args, **kwds):
        super(PyXKB, self).__init__(*args, **kwds)

        # 防止死循环调用 __getattr__
        self.xkb = None
        
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
            self.xkb = XKB
            return self

        if loginDict is None:
            return self

        self.xkb = XKB.KB()
        self.xkb.login(loginDict)
        return self

    def _use(self, strTable):

        self.xkb.use(strTable)
        self.strUsedTableSet.add(strTable)
        return self

    def login(self, loginDict, bUsePool = False):
        try:
            self._login(loginDict, bUsePool)
        except Exception as e:
            print "[ERROR] XKB -> login"
            raise e

    def use(self, strTable):
        try:
            self._use(strTable)
        except Exception as e:
            print "[ERROR] XKB -> use"
            raise e

    def callback(self, *args, **kwds):

        self.login(self.loginDict, self.bUsePool)

        if self.bUsePool is False:
            for strUsedTable in self.strUsedTableSet:
                self.use(strUsedTable)

        return args, kwds

    def __getattr__(self, attr):
        res = getattr(self.xkb, attr, None)
        if res is None:
            return super(PyXKB, self).__getattribute__(attr)
        return res
