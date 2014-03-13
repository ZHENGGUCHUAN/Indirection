# -*- coding: UTF-8 -*-
'''
Created on 2013-11-29

@author: Grayson
'''
import sys, os
from constant import dbDict
from collections import OrderedDict
sys.path.append(os.getcwd() + r'\..\Common')
from mssqlAPI import MssqlAPI
from mongoAPI import MongoAPI
from dbServer import mssqlDbServer, mongoDbServer


class SplitSecurity(object):
  '''
  Split security index and insert into mongoDB.
  '''


  def __init__(self, mssqlConnDict, mongoConnDict):
    '''
    Constructor MSSQL handle.
    '''
    self.mssqlHandle = MssqlAPI(mssqlConnDict['SERVER'], mssqlConnDict['DB'], mssqlConnDict['USER'], mssqlConnDict['PWD'])
    self.mongoHandle = list()
    for mongo in mongoConnDict:
      self.mongoHandle.append(MongoAPI(mongo['SERVER'], mongo['PORT']))
    
    
  def __del__(self):
    '''
    Destructor, delete class variable.
    '''
    try:
      del self.mssqlHandle
    except:
      pass
    
    try:
      for mongoHandle in self.mongoHandle:
        del mongoHandle
    except:
      pass
    
    
  def GetSecurityCodeList(self, db):
    '''
    Get security code list from Qianniu database.
    #从千牛基础库信息中读取包含证券列表信息。
    '''
    sqlCmd = '''
      SELECT
        INNER_CODE 
      FROM 
        ''' + dbDict[db]['TABLE']['ANA'] + ''' 
      GROUP BY 
        INNER_CODE 
      ORDER BY 
        INNER_CODE      
      '''
    records = self.mssqlHandle.sqlQuery(sqlCmd)
    securityCodeList = list()
    for rec in records:
      securityCodeList.append(rec['INNER_CODE'])
    return securityCodeList
    
    
  def SplitSecuritySubarea(self, securityCodeList, num):
    '''
    Split security index, return split list.
    '''
    securityNum = len(securityCodeList)
    splitNum = securityNum // num
    if (securityNum % num > 0):
      splitNum += 1
    
    indexList = list()
    minValue = int(securityCodeList[0])
    maxValue = 0
    for sec in securityCodeList:
      index = securityCodeList.index(sec)
      maxValue = int(sec)
      if (index % splitNum == splitNum-1):
        indexList.append({'min':minValue,'max':maxValue})
        minValue = maxValue + 1
    else:
      indexList.append({'min':minValue,'max':maxValue})
    return indexList
  
  
  def SplitSecurityIndex(self, securityCodeList, num):
    indexList = self.SplitSecuritySubarea(securityCodeList, num)
    splitDict = dict()
    for sec in securityCodeList:
      secCode = int(sec)
      for index in indexList:
        if ((secCode >= index['min']) and (secCode <= index['max'])):
          idxStr = str(indexList.index(index))
          while (len(idxStr) < 3):
            idxStr = '0' + idxStr
          splitDict[sec] = idxStr
    return splitDict
  
    
if __name__ == '__main__':
  splitSecurityIndex = SplitSecurity(mssqlDbServer['251']['WAN'], (mongoDbServer['23'],))
  splitDict = {'DAY':200, 'WEEK':100, 'MONTH':50}
  for item in splitDict:
    securityCodeList = splitSecurityIndex.GetSecurityCodeList(item)
    #splitRangeList = splitSecurityIndex.SplitSecuritySubarea(securityCodeList, splitDict[item])
    splitIndex = splitSecurityIndex.SplitSecurityIndex(securityCodeList, splitDict[item])
    print splitIndex
    '''
    num = 0
    for mongoHandle in splitSecurityIndex.mongoHandle:
      mongoHandle.drop('SplitIndex', item)
    for range in splitRangeList:
      document = OrderedDict()
      document['_id'] = num
      document['Range'] = [range['min'], range['max']]
      #print num,'min:',range['min'],', max:',range['max']
      for mongoHandle in splitSecurityIndex.mongoHandle:
        mongoHandle.insert('SplitIndex', item, document)
      num += 1
    '''