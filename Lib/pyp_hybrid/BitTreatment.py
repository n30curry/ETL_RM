# -*- coding:utf-8 -*- 

# 使用 UTF-8
import sys
reload(sys)
sys.setdefaultencoding( "utf-8" )

def intOffset(integer, iByteOffset, iLength = None):
	"""
		获取数值类型前(后)几个字节对应的数字
		输入：
			integer int/long 需要截断的数字
			iByteOffset int/long 截断的长度及位置
				如果 iByteOffset > 0 从高位起截断 iByteOffset 个字节
				如果 iByteOffset < 0 从低位起截断 iByteOffset 个字节
				如果 iByteOffset = 0 不进行截断，直接返回 long 类型的 integer
			iLength int 数字 integer 真实的字节数(可选)
				如果不写则按 integer 当前字节数计算
		输出：
			截断后的 long类型 10进制 数字
	"""
	if iByteOffset == 0:
		return long(integer)

	strIntegerHex = str(hex(long(integer)))[2:-1]
	iIntegerHexLength = len(strIntegerHex)

	iLength = iLength or iIntegerHexLength
	strIntegerHex = "".join(["0"*(iLength - iIntegerHexLength), strIntegerHex])

	try:
		return long(strIntegerHex[:iByteOffset] if iByteOffset > 0 else strIntegerHex[iByteOffset:], 16)
	except IndexError as e:
		return long(strIntegerHex, 16)

def intSplit(integer, iPos, iLength = None):
	"""
		按位切割数字
		输入：
			integer int/long 被切割的数字
			iPos int 从第 iPos 个字节切割数字
			iLength int 数字 integer 真实的字节数(可选)
				如果不写则按 integer 当前字节数计算
		输出：
			包含 (高位数字，低位数字) 的 Tuple 类型，其中：
				高位数字为 long 类型
				低位数字为 long 类型
	"""
	strIntegerHex = str(hex(long(integer)))[2:-1]

	if iPos == 0:
		return "", strIntegerHex

	iIntegerHexLength = len(strIntegerHex)
	iLength = iLength or iIntegerHexLength
	strIntegerHex = "".join(["0"*(iLength - iIntegerHexLength), strIntegerHex])

	return long(strIntegerHex[:iPos], 16), long(strIntegerHex[iPos:], 16)

def intStitch(former, latter, iLatterLength = None):
	"""
		拼接两个数字
		输入：
			former int/long 组成高位的数字
			latter int/long 组成低位的数字
			iLatterLength int 低位数字的长度
		输出：
			拼接后的 long类型 10进制 数字
	"""
	strFormerHex = str(hex(long(former)))[2:-1]
	strLatterHex = str(hex(long(latter)))[2:-1]
	return long("".join([strFormerHex, "0"*(iLatterLength - len(strLatterHex)), strLatterHex]), 16)

# def walkDFSIndex(data):
# 	for sliceDict in data:
# 		excludeList = set(sliceDict['xlist'])
# 		for iPositionInteger in sliceDict['list']:
# 			# iBeginPosition = intOffset(iPositionInteger, -8, 16)
# 			# iEndPosition = intOffset(iPositionInteger, 8, 16)
# 			iEndPosition, iBeginPosition = intSplit(iPositionInteger, 8, 16)
# 			for iPosition in xrange(iBeginPosition, iEndPosition+1):
# 				if iPosition in excludeList:
# 					continue
# 				yield intStitch(sliceDict['sid'], iPosition, 8)

# if __name__ == "__main__":
#         data = [{'list': [42949672961L], 'xlist': [], 'sid': 1L}]
#         for index in walkDFSIndex(data):
#                 print index


