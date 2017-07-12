#!/usr/bin/python
# -*- coding: utf-8 -*-

# import sys
# reload(sys)
# sys.setdefaultencoding("utf-8")


# # from Lib.pyp import Util
# import os
# import platform
# import queryfz

# PYP_LIBRARY_PATH = './Lib/pyp'

# str_platform_system = platform.system()
# if str_platform_system == "Linux":

#     bReload = True
#     strLibPath = os.path.join(os.getcwd(), PYP_LIBRARY_PATH)

#     if "LD_LIBRARY_PATH" not in os.environ:
#         os.environ["LD_LIBRARY_PATH"] = strLibPath
#     elif strLibPath not in os.environ["LD_LIBRARY_PATH"]:
#         os.environ["LD_LIBRARY_PATH"] += ":" + strLibPath
#     else:
#         bReload = False

#     if bReload is True:
#         try:
#             os.execv(sys.argv[0], sys.argv)
#         except Exception as exc:
#             print ('Failed re-exec:' + str(exc))
#             sys.exit(1)

# elif str_platform_system == "Windows":
    
#     strLibPath = os.path.join(os.getcwd(), PYP_LIBRARY_PATH)
    
#     if "PATH" not in os.environ:
#         os.environ["PATH"] = strLibPath
#     elif strLibPath not in os.environ["PATH"]:
#         os.environ["PATH"] += ";" + strLibPath


from Lib.pyp import KDB
from Lib.pyp import NDB
from Lib.pyp import Num

NDB_HOST = '192.168.0.205'
NDB_PORT = 9860
NDB_TABLE = 'test'
KDB_HOST = "192.168.0.208"
KDB_PORT = 9910
KDB_TABLE = 'kdb_rmlink'

class Test(object):
    """docstring for Test"""
    def __init__(self):
        super(Test, self).__init__()

        self.ndb = NDB.DB()
        self.ndb.login({'host':NDB_HOST, 'port':NDB_PORT})
        self.ndb.use(NDB_TABLE)

        self.kdb = KDB.DB()
        self.kdb.login({'host':KDB_HOST, 'port':KDB_PORT})
        self.kdb.use(KDB_TABLE)

    def transform(self, nodes, links):

        links = map(lambda x: Num.split(x), links)

        rm_links = []
        for i in links:

            rm_links.append([nodes[i[0]], nodes[i[1]]])

        for link in rm_links:
            for i in range(len(link)):
                address = self.ndb.maps(NDB_TABLE,[link[i]])['keys']
                link[i] = address

        end_links = []
        # print "RM_LINK",rm_links
        for link in rm_links:
            end_links.append(link[0][0] + '\x00' + link[1][0])

        scene = []
        for i in end_links:
            scene.append(self.kdb.select(KDB_TABLE, i)['scene'])

        scenes = map(lambda x:self.reverse(x), scene)

        return dict(zip(end_links,scenes))

    def reverse(self,scenes):
        property_list = []
        for scene in scenes:

            iscenerole, num = Num.split(scene)

            hscenerole = hex(iscenerole)[2:]
            hscene, role = hscenerole[:-1], hscenerole[-1]

            iscene = int(hscene, 16)

            property_list.append([iscene, int(role), num])

        return property_list
    def test(self,scene_dict):
        d = {}
        for k,v in scene_dict.items():
            for scene in v:
                if scene[0] not in [111,112,113]:
                    d[k] = v
                    break
        return d
        
if __name__ == "__main__":

    test = Test()
    # l = test.transform([76275153591171248L, 76534715827861085L, 83177236239202493L, 83417420425080559L, 84271237261735612L, 85972692776870514L, 86706549682893215L, 8681018092803354212L, 618907258259862771L, -1609752010221797153L, -6543421894337922668L, -4862539875529774664L, -8704760567805457105L],[8589934599L, 12884901896L, 12884901897L, 17179869194L, 21474836491L, 25769803788L])
    # print l
    print test.reverse([115964126979L])
