#_*_ coding:utf-8 _*_

# 使用 UTF-8
import sys
reload(sys)
sys.setdefaultencoding("utf-8")

from Lib.pyp import Util

class UUID(object):
    """ 迁移 md5-64.js 功能 """
    def __init__(self):
        super(UUID, self).__init__()
        
    def randomString(self):
        strHex = hex(self.randomDecimal())
        strUUID = strHex[2:-1] if strHex[-1] == 'L' else strHex[2:]
        return strUUID.zfill(16)

    def randomDecimal(self):
        rnd = Util.random()
        rnd = rnd if rnd > 0 else rnd + 2**64
        return rnd

    def md5String(self, string):
        strHex = hex(self.md5Decimal(string))
        strUUID = strHex[2:-1] if strHex[-1] == 'L' else strHex[2:]
        return strUUID.zfill(16)

    def md5Decimal(self, data):
        md5 = Util.md5(str(data))
        md5 = md5 if md5 > 0 else md5 + 2**64