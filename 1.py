#!/usr/numin/python

'''
from Linum.pyp_hynumrid.PyKDnum import PyKDnum

kdnum = PyKDnum()
kdnum.login({'host':'192.168.0.237','port':9910})
kdnum.use('v63xj_kdnum_rmlink')
a = [{'count': [[0, 40590L]], 'xcount': [[0, 8634857L]], 'sid': 65551}, {'count': [[0, 231493L]], 'xcount': [[0, 8139019L]], 'sid': 65552}, {'count': [[0, 50138L]], 'xcount': [[0, 8520006L]], 'sid': 65549}, {'count': [[0, 115302L]], 'xcount': [[0, 8273069L]], 'sid': 65550}, {'count': [[0, 178965L]], 'xcount': [[0, 8428176L]], 'sid': 131080}, {'count': [[0, 86160L]], 'xcount': [[0, 8543159L]], 'sid': 131078}, {'count': [[0, 0L]], 'xcount': [[0, 8311913L]], 'sid': 131079}, {'count': [[0, 38816L]], 'xcount': [[0, 8240347L]], 'sid': 131077}, {'count': [[0, 156825L]], 'xcount': [[0, 8303803L]], 'sid': 196614}, {'count': [[0, 44875L]], 'xcount': [[0, 8431094L]], 'sid': 196612}, {'count': [[0, 148985L]], 'xcount': [[0, 8238602L]], 'sid': 196613}, {'count': [[0, 176290L]], 'xcount': [[0, 8322640L]], 'sid': 196611}, {'count': [[0, 25830L]], 'xcount': [[0, 8507538L]], 'sid': 262150}, {'count': [[0, 189228L]], 'xcount': [[0, 8314695L]], 'sid': 262147}, {'count': [[0, 74398L]], 'xcount': [[0, 8352997L]], 'sid': 262148}, {'count': [[0, 73374L]], 'xcount': [[0, 8162211L]], 'sid': 262149}, {'count': [[0, 0L]], 'xcount': [[0, 0L]], 'sid': 327685}, {'count': [[0, 178965L]], 'xcount': [[0, 8428330L]], 'sid': 327687}, {'count': [[0, 27952L]], 'xcount': [[0, 8052360L]], 'sid': 327684}, {'count': [[0, 151989L]], 'xcount': [[0, 7855601L]], 'sid': 327686}, {'count': [[0, 156825L]], 'xcount': [[0, 8307360L]], 'sid': 393222}, {'count': [[0, 29520L]], 'xcount': [[0, 8367612L]], 'sid': 393223}, {'count': [[0, 173376L]], 'xcount': [[0, 8046631L]], 'sid': 393220}, {'count': [[0, 59398L]], 'xcount': [[0, 8166199L]], 'sid': 393221}, {'count': [[0, 151434L]], 'xcount': [[0, 8104330L]], 'sid': 458757}, {'count': [[0, 48520L]], 'xcount': [[0, 8240042L]], 'sid': 458755}, {'count': [[0, 38434L]], 'xcount': [[0, 8046567L]], 'sid': 458756}, {'count': [[0, 167712L]], 'xcount': [[0, 7973464L]], 'sid': 458758}, {'count': [[0, 197450L]], 'xcount': [[0, 8219091L]], 'sid': 524295}, {'count': [[0, 28720L]], 'xcount': [[0, 8430192L]], 'sid': 524294}, {'count': [[0, 152030L]], 'xcount': [[0, 8206865L]], 'sid': 524292}, {'count': [[0, 48916L]], 'xcount': [[0, 8167277L]], 'sid': 524293}]
kdnum.traverse('v63xj_kdnum_rmlink',a,check_path='1.checkpoint')
'''
# c = [[],[]]
# d = []
# e = []
# v = 0
# a = ['1#13118783143', '1#13118783143', '1#13118783138', '1#13118783138', '1#13118783122', '1#13118783122', '1#13118783130', '1#13118783130', '1#13118783169', '1#13118783169', '1#13118783190', '1#13118783190']

# num = [[0, 1040522936, 0, 1040522936, 0, 1040522936, 0, 1040522936, 0, 1040522936, 1040522936, 0], [0, 1056964608, 0, 1056964608, 0, 1056964608, 0, 1056964608, 0, 1056964608, 1056964608, 0], [], []]

# keys = []
# for i in a:
# 	if i not in keys:
# 		keys.append(i)

# for i in keys:
# 	for j in a:
# 		if i == j:
# 			v +=1
# 			print v
# 	for j in range(v):
# 		d.append(num[0][0])
# 		e.append(num[1][0])
# 		del num[0][0]
# 		del num[1][0]
# 	v = 0
# 	c[0].append(d)
# 	c[1].append(e)
# 	d = []
# 	e = []

# print c



import xml.etree.ElementTree as Etreer

# tree = Etreer.parse('data42.xml')
#         root = tree.getroot()
#         tag = root.tag

#         pattern = {}
#         nodedict = {}
#         for scene in root.findall('scene'):
#             sid = scene.get('id')
#             weight = scene.get('weight')
#             pattern[sid] = {}

#             pattern[sid]['weight'] = weight
#             #name = json.dumps(scene.get('name'),ensure_ascii=False)
#             #print sid,name
#             for ty in scene.findall('type'):
#                 typeid = ty.get('id')
#                 pattern[sid][typeid] = []

#                 for link in ty.findall('link'):
#                     start = link.get('start')
#                     end = link.get('end')
#                     o0 = link.get('o0')
#                     d0 = link.get('d0')
#                     weight = link.get('weight')
#                     pattern[sid][typeid].append({'start':start,'end':end, 'o0':o0, 'd0':d0, 'weight':weight})

#         for node in root.findall('node'):
#             sid = node.get('id')
#             c0 = node.get('c0')
#             l0 = node.get('l0')
#             nodedict[sid] = [c0,l0]


# tree = Etreer.parse('data42.xml')

# root = tree.getroot()
# tag = root.tag

# pattern = {}
# for scene in root.findall('scene'):
# 	d = {}
# 	weight = float(scene.get('weight'))
# 	sid = int(scene.get('id'))
# 	o0 = float(scene.get('o0'))
# 	d['weight'] = weight
# 	d['o0'] = o0
# 	pattern[sid] = d

# print pattern

# role_dict = {}
# for role in root.findall('role'):
# 	weight = float(role.get('weight'))
# 	sid = int(role.get('id'))
# 	role_dict[sid] = weight

# nodedict = {}

# for node in root.findall('node'):
# 	sid = int(node.get('id'))
# 	c0 = int(node.get('c0'))
# 	nodedict[sid] = c0

# print role_dict
# print nodedict

# import time
# while True:
# 	try:
# 		a = ['a','b','c']
# 		a[10]
# 	except:
# 		print "time"
# 		time.sleep(2)
# 		continue


# class a():
# 	pass


