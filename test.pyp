# -*- coding: utf-8 -*-
import NDB

ndb = NDB.DB()

table = "test"

ndb.login({"host":"localhost", "port":9869, "timeout":300})
#ndb.login({"host":"192.168.0.195", "port":9949, "timeout":300})

ndb.use("test")


print("*"*70)

keys = ["a","b","c","d"]
ranks = [1,2,3,4]


# keys = ['a','b']
# ranks= [1,2]

a = ndb.inserts(table, keys,ranks)
print a



kid1 = a["kids"][0]
print kid1
kids1 =[a["kids"][1]] 
print kids1
nums1 = [100]


ndb.insertX(table,kid1,kids1,nums1)
#ndb.dumps(table)

kids2 = [a["kids"][0],a["kids"][1]]
xkids2 =[[a["kids"][2]],[a["kids"][3]]] 
xnums2 = [[100],[200]]


ndb.insertsX(table,kids2,xkids2,xnums2)




# print ndb.keyCount(table,1)

#print ndb.select(table,"a")
# #print ndb.maps()
# print ndb.shards(table)



# rs1 =  ndb.selects(table,["a","b"])
# print rs1 



"""
select_dict = {'kids':['a','b'],'xkids':[['c','d'],['c','e']]}

rs1 =  ndb.selects(table,["a","b"])
print rs1

def reverse(select_dict):
    node_list, link_list = [], []
    kids = select_dict['kids']
    xkids = select_dict['xkids']
    for ipos, links in enumerate(xkids):
        for node in links:
            temp_list = []
            node_list.append(node)
            for ipos1, links1 in enumerate(xkids):
                if node in links1:
                    temp_list.append(kids[ipos1])
            link_list.append(temp_list)

    graph_dict = dict(zip(node_list, link_list))

    return graph_dict

graph_dict = reverse(select_dict)
node_list, link_list = [], []
for k,v in graph_dict.items():
    node_list.append(k)
    link_list.append(v)

print node_list
print link_list
   
"""



ndb.logout()
