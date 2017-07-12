#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
reload(sys)
sys.setdefaultencoding("utf-8")

from Lib.pyp import Num

def properties(docs):

    properties = []
    for scenes in map(lambda x: x["scene"], docs):

        property_list = []
        for scene in scenes:

            iscenerole, num = Num.split(scene)

            hscenerole = hex(iscenerole)[2:]
            hscene, role = hscenerole[:-1], hscenerole[-1]

            iscene = int(hscene, 16)

            property_list.append([iscene, int(role), num])

        properties.append(property_list)

    return properties