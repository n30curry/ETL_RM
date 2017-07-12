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

kdb = PyKDB()
kdb.login({'host':'192.168.0.208','port':9910})
kdb.use('kdb_rmlink')
kdb_list = [
'1#13195235531\x001#15117764326', '1#13195235531\x001#15185361271', '1#13195235531\x001#18788656263', '1#13195235531\x0011#460015239928633', 
 '1#13195256957\x001#13708558434',
 '1#13195256957\x0011#460015548317868', '1#13195235825\x001#13885337346', '1#13195235825\x001#15117762718', 
'1#13195235825\x001#15185396495', '1#13195235825\x001#18083388258', '1#13195235825\x001#18224746775', '1#13195235825\x001#18722709533',
]
docs = kdb.kdb.selects('kdb_rmlink', kdb_list)
print docs

a = ['1#13195235531\x001#15117764326', '1#13195235531\x001#15185361271', '1#13195235531\x001#18788656263', '1#13195235531\x0011#460015239928633',]