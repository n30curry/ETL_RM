#_*_ coding:utf-8 _*_

# 使用 UTF-8
import sys
reload(sys)
sys.setdefaultencoding("utf-8")

##########################################
# 程序运行入口代码
"""

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

"""
###########################################

import os
import re

SVN_REPO_LINUX = "svn://192.168.0.217/repository/product/linux64/release/xdp_server/bin"
SVN_REPO_WINDOWS = "svn://192.168.0.217/repository/product/windows/release/xdp_server/bin"

class pyp_autoLoader(object):
    """
        TODO: 同步加载 windows 动态链接库
    """
    def __init__(self, dest = None, isUpdate = False):
        super(pyp_autoLoader, self).__init__()

        self.svnRepo = SVN_REPO_LINUX
        self.strTemporaryPath = ".pyp_tmp"

        self.svnRepo_win = SVN_REPO_WINDOWS
        self.strTemporaryPath_win = ".pyp_tmp_win"

        self.dest = dest or "pyp"
        self.isUpdate = isUpdate

        self.pkgList = []

    def initTemporaryFolder(self):

        strCommand = " ".join(["svn","checkout", self.svnRepo, self.strTemporaryPath])
        os.system(strCommand)

        strCommand = " ".join(["svn","checkout", self.svnRepo_win, self.strTemporaryPath_win])
        os.system(strCommand)

        strEntryPath = os.path.join(self.strTemporaryPath, "libxdk_pyp.so")
        strEntryAliasPath = os.path.join(self.strTemporaryPath, "pyp.so")
        os.system(" ".join(["cp", strEntryPath, strEntryAliasPath]))

        strEntryPath = os.path.join(self.strTemporaryPath_win, "xdk_pyp.dll")
        strEntryAliasPath = os.path.join(self.strTemporaryPath_win, "pyp.pyd")
        os.system(" ".join(["cp", strEntryPath, strEntryAliasPath]))

    def getPkgLibraryList(self, strPkgList):
        strLoadList = []
        for strFile in strPkgList:
            strFile = strFile.strip()
            if strFile.endswith(".so") is True:
                strLoadList.append(strFile)
            else:
                strLoadList.append("".join(["libpyp_", strFile.lower(), ".so"]))

        return strLoadList

    def getDependency(self, LibraryFilePath, exclude = None):
        """
            寻找 strLibraryFilePath 文件的动态链接库依赖，并返回结果列表
            结果列表中会排除 exclude 中的项
        
            Input :
                strLibraryFilePath : String/List
                    需要查询依赖的动态链接库文件路径
                exclude : List
                    与要排除的动态链接库列表
            Return : List
        """

        if isinstance(LibraryFilePath, str) is True:
            LibraryFilePath = [LibraryFilePath]

        dependencyList = []
        for strLibFilePath in LibraryFilePath:
            dependencyList.extend(self.getDependencySingle(strLibFilePath))
        
        return list(set(dependencyList) - set([] if exclude is None else exclude))

    def getDependencySingle(self, strLibraryFilePath, exclude = None):
        """
            寻找 strLibraryFilePath 文件的动态链接库依赖，并返回结果列表
            结果列表中会排除 exclude 中的项
        
            Input :
                strLibraryFilePath : String
                    需要查询依赖的动态链接库文件路径
                exclude : List
                    与要排除的动态链接库列表
            Return : List

        """

        pattern = re.compile(r"\s*(lib\w+_\w+\.so)\s+=>")
        
        dependencyList = []

        strLibraryFilePath = os.path.join(self.strTemporaryPath, strLibraryFilePath)
        for strLine in os.popen("ldd " + strLibraryFilePath).readlines():
            res = pattern.search(strLine)
            if res is None:
                continue

            dependencyList.append(res.groups()[0])

        return list(set(dependencyList) - set([] if exclude is None else exclude))

    def _copyFiles(self, strFileList):

        for strFile in strFileList:
            strFileSrc = os.path.join(self.strTemporaryPath, strFile)
            strFileDest = os.path.join(self._dest, strFile)
            strCommand = " ".join(["cp", strFileSrc, strFileDest])
            os.system(strCommand)

    def _copyFilesCovertWindows(self, strFileList):

        for strFile in strFileList:

            if strFile == "pyp.so":
                strFile_win = "pyp.pyd"
            elif strFile.startswith("lib") is False:
                raise Exception("ERROR: Wrong Linux so file")
            else:
                strFile_win = "".join([strFile[3:-3], ".dll"])

            print "{0} \t\t\t {1}".format(strFile, strFile_win)

            strFileSrc = os.path.join(self.strTemporaryPath_win, strFile_win)
            strFileDest = os.path.join(self._dest, strFile_win)
            strCommand = " ".join(["cp", strFileSrc, strFileDest])
            os.system(strCommand)

    def load(self, strPkgList, exclude = None):
        """
            自动加载需要的 pyp 动态链接库
        
            Input :
                strPkgList : List
                    需要加载的包名称列表
                    例如：['Util','Xfs']

            Return : List

        """
        if os.path.exists(self.strTemporaryPath) is False:
            self.initTemporaryFolder()

        self.pkgList.extend(strPkgList)

        strLoadList = self.getPkgLibraryList(strPkgList)
        strDependencyList = self.getDependency(strLoadList, exclude)

        while True:
            furtherDependency = self.getDependency(strDependencyList, exclude)

            if len(strDependencyList) == len(set(strDependencyList + furtherDependency)):
                break

            strDependencyList = list(set(strDependencyList + furtherDependency))

        self._copyFiles(strLoadList)
        self._copyFiles(strDependencyList)
            
        self._copyFilesCovertWindows(strLoadList)
        self._copyFilesCovertWindows(strDependencyList)

    def flush(self):

        pkgList = list(set(self.pkgList))

        strLoadPkgList = []
        strImportPkgList = []
        for strPkgName in pkgList:
            strPkgName = strPkgName.strip()
            if strPkgName.endswith(".so") is True:
                continue
            strLoadPkgList.append(strPkgName.lower())
            strImportPkgList.append(strPkgName)

        strFile = os.path.join(self.dest, "__init__.py")
        with open(strFile, "w") as fileObject:
            fileObject.write("# -*- coding:utf-8 -*-\n")
            fileObject.write("\n#####################\n")
            fileObject.write("\n# 此文件为系统自动生成，请勿改动\n")
            fileObject.write("\n#####################\n\n")


            # fileObject.write("import os\nimport platform\n")
            # fileObject.write('if platform.system() != "Linux":\n')
            # fileObject.write('    os.chdir("./Lib/pyp")\n\n\n')
    
            fileObject.write("import pyp\n\n")

            if "xkm" in strLoadPkgList:
                fileObject.write('pyp.kmLoad("xkm")\n\n')

            strLoadList = map(lambda x: 'pyp.load("{0}")'.format(x), strLoadPkgList)

            if "Util" in strImportPkgList:
                strImportPkgList.extend(["Num", "Json", "Zip"])

            strImportList = map(lambda x: 'import {0}'.format(x), strImportPkgList)
            
            fileObject.write("\n".join(strLoadList))
            fileObject.write("\n\n")
            fileObject.write("\n".join(strImportList))

            # fileObject.write('\n\n\nif platform.system() != "Linux":\n')
            # fileObject.write('    os.chdir("..")\n    os.chdir("..")')

    @property
    def dest(self):
        return self._dest
    @dest.setter
    def dest(self, dest):
        if dest is None:
            return None

        self._dest = dest

        if os.path.exists(dest) is False:
            os.mkdir(dest)

    def __del__(self):

        if os.path.exists(self.strTemporaryPath) is True:
            os.system("rm -rf " + self.strTemporaryPath)

        if os.path.exists(self.strTemporaryPath_win) is True:
            os.system("rm -rf " + self.strTemporaryPath_win)

        if self.isUpdate is False:
            self.flush()

autoLoader = pyp_autoLoader("./pyp")
# autoLoader = pyp_autoLoader("./pyp", isUpdate = True)
autoLoader.load(["pyp.so","Util","XFS","IDB","GDB","MDB","FDB", "KDB", "BDB", "TDB","NDB","XKB","XDP", "XKM"])
