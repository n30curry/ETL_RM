#!/usr/bin/python
# -*- coding: UTF-8 -*-

# import mapr.pattern as Pattern
# import pattern as Pattern
# import os, json


# pattern = Pattern.xmlRead("pattern1_A.xml")
# print(pattern)

import xml.etree.ElementTree as Etreer
import json

tree = Etreer.parse('pei.xml')
root = tree.getroot()
tag = root.tag

# for child in root:
# 	print child.tag,child.attrib
pattern = {}
for scene in root.findall('scene'):
	sid = scene.get('id')
	pattern[sid] = {}
	#name = json.dumps(scene.get('name'),ensure_ascii=False)
	#print sid,name
	for ty in scene.findall('type'):
		typeid = ty.get('id')
		pattern[sid][typeid] = []

		for link in ty.findall('link'):
			start = link.get('start')
			end = link.get('end')
			o0 = link.get('o0')
			d0 = link.get('d0')
			weight = link.get('weight')
			pattern[sid][typeid].append({'start':start,'end':end, 'o0':o0, 'd0':d0, 'weight':weight})

print pattern		


def xmlRead(fname):
	pattern = {
	"attr": {},
	"scenes": []
	}

	tree = Etree.parse(fname)
	root = tree.getroot()

	for node in root.iter("scene"):
		pattern["attr"]["id"] = node.attrib.get("id", "")
		print node
		xmlGetScene(pattern["scenes"], node)

	return pattern;

