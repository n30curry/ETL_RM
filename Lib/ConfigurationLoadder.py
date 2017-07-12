# -*- coding: utf-8 -*-

import re
import sys
import json

reload(sys)
sys.setdefaultencoding("utf-8")

class ConfigurationLoadder(object):
    """docstring for ConfigurationLoadder"""
    def __init__(self):
        super(ConfigurationLoadder, self).__init__()

        self.configTreatmentDict = {
            "json" : self._loadJson
        }

    def load(self, strFilePathName):

        with open(strFilePathName,"r") as fileObject:
            strLine = fileObject.readline().strip()
            iLastPosition = fileObject.tell()
            while len(strLine) == 0:
                strLine = fileObject.readline().strip()
                iCurrentPosition = fileObject.tell()
                if iLastPosition == iCurrentPosition:
                    return {}
                iLastPosition = iCurrentPosition

            res = re.match(r"#\s*-\*-\s*[tT][yY][pP][eE]\s*:\s*([^\s]*)\s*-\*-", strLine)

            strConfigType =  res.groups()[0].lower()

            if strConfigType not in self.configTreatmentDict:
                raise Exception("Unknown Configuration File Type")

            return self.configTreatmentDict[strConfigType](fileObject.readlines())

    def _loadJson(self, strConfigList):
        return eval("".join(strConfigList))
