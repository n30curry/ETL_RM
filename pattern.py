#!/usr/bin/python
# -*- coding: UTF-8 -*-

# import sys
# reload(sys)
# sys.setdefaultencoding("utf-8")

# import os
# import platform

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

import xml.etree.ElementTree as Etree
from Lib.pyp import Util, Num
# from Lib.pyp import File

ids = ["id", "cell", "mail", "qq", "weibo", "imei", "cellcode"]

def xmlGetProp(root):
	link = {}

	link["weight"] = root.attrib.get("weight", "0")

	return link

def xmlGetLink(links, mid, root):
	# for node in root.iter("end"):
	for node in root.iter("link"):
		nid = node.attrib.get("id", "")

		pid = ids.index(mid)
	 	sid = ids.index(nid)
		kid = Num.merge(pid, sid, 0)

		links[kid] = xmlGetProp(node)

def xmlGetScene(scenes, root):
	for node in root.iter("start"):
		scene = {
			"attr": {},
			"links": {}
		}

		nid = node.attrib.get("id", "")
		xmlGetLink(scene["links"], nid, node)

		scene["attr"]["name"] = node.attrib.get("name", "")
		scenes.append(scene)

def xmlRead(fname):
	pattern = {
	"attr": {},
	"scenes": []
	}

	tree = Etree.parse(fname)
	root = tree.getroot()

	for node in root.iter("scene"):
		pattern["attr"]["attribute"] = node.attrib.get("attribute", "")
		print node
		xmlGetScene(pattern["scenes"], node)

	return pattern;

xmlRead('pattern.xml')
