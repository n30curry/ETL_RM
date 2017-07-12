#!/usr/bin/python
# -*- coding: utf-8 -*-

from Lib.pyp import Num

class Query(object):
	def __init__(self):
		super(Query, self).__init__()

	data = {
		"graph": {
			"nodes": [],
			"nums": [],
			"links": [],
			"xnums": []
		},
		"kids": [],
		"sets": {}
	}

	def addKey(self, kid, flag):
		kids = self.data["sets"].keys()

		if kid in kids:
			id = self.data["sets"][kid]
		else:
			id = len(kids);
			self.data["sets"][kid] = id

			self.data["graph"]["nodes"].append(kid)
			self.data["graph"]["nums"].append(0)

			if flag:
				self.data["kids"].append(kid)

		return id

	def addKeys(self, kids):
		for i in range(len(kids)):
			self.addKey(kids[i], False)

	def addLinks(self, pid, kids, nums):
		for i in range(len(kids)):
			id = self.addKey(kids[i], True)
			self.data["graph"]["links"].append(Num.merge(id, pid, True))
			self.data["graph"]["xnums"].append(nums[i])

	def addNode(self, node, num, links, nums):
		id = self.data["sets"][node]
		self.data["graph"]["nums"][id] = num
		self.addLinks(id, links, nums)

	def parse(self, kids, rids, nums, xkids, xnums):
		for i in range(len(rids)):
			node = kids[rids[i]]
			self.addNode(node, nums[i], xkids[i], xnums[i])
