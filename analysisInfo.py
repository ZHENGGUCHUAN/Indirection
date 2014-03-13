# -*- coding: UTF-8 -*-
'''
Created on 2013-12-06

@author: Grayson
'''
from constant  import dbDict
import sys, os
sys.path.append(os.getcwd() + r'\..\Common')
from mssqlAPI import MssqlAPI
from logFile  import LogFile


class AnalysisInfo(object):
  '''
  Security analysis info
  '''


  def __init__(self, server, db, user, pwd):
    '''
    Constructor
    '''
    self.mssqlHandle = MssqlAPI(server, db, user, pwd)
    
    
  def __del__(self):
    del self.mssqlHandle
    
    
  
  def GetSpecifyAnalysisInfo(self, db, codeList, queue):
    condition = 'INNER_CODE IN ('
    for code in codeList:
      condition += "'" + str(code) + "',"
    condition = condition[0:-1]
    condition += ')'
    if ((db == 'WEEK') and (db == 'MONTH')):
      condition += ' AND [SECURITY_ANALYSIS].[TRADE_DAYS] > 0'
    self.__getAnalysisInfo(db, condition, queue)
    
    
  def GetAllAnalysisInfo(self, db, queue):
    if ((db == 'WEEK') and (db == 'MONTH')):
      condition = '[SECURITY_ANALYSIS].[TRADE_DAYS] > 0'
    else:
      condition = '1 = 1'
    self.__getAnalysisInfo(db, condition, queue)
    
    
  def __getAnalysisInfo(self, db, condition, queue):
    '''
    private method
    #获取指定证券代码列表所示分析信息，以单个证券代码为单位由queue传递出去
    #传出消息按如下字典格式:
    {'DB'  :'DAY/WEEK/MONTH',
     'MSG' :[{'INNER_CODE':<INNER_CODE>,
              'LCLOSE':<LCLOSE1>,
              'TOPEN':<TOPEN1>,
              'TCLOSE':<TCLOSE1>,
              'THIGH':<THIGH1>,
              'TLOW':<TLOW1>,
              'TVOLUME':<TVOLUME1>,
              'TVALUE':<TVALUE1>,
              ... ...
             },
             {'INNER_CODE':<INNER_CODE>,
              'LCLOSE':<LCLOSE2>,
              'TOPEN':<TOPEN2>,
              'TCLOSE':<TCLOSE2>,
              'THIGH':<THIGH2>,
              'TLOW':<TLOW2>,
              'TVOLUME':<TVOLUME2>,
              'TVALUE':<TVALUE2>,
              ... ...},
             ... ...]
    }
    '''
    logFile = LogFile(name = 'Indirection')
    try:
      if ((db == 'WEEK') or (db == 'MONTH')):
        field = '''
          [SECURITY_ANALYSIS].[FIRST_TRADE_DATE] AS FDATE,
          [SECURITY_ANALYSIS].[LAST_TRADE_DATE] AS LDATE,
        '''
      else:
        field = '''
          [SECURITY_ANALYSIS].[TRADE_DATE] AS FDATE,
          [SECURITY_ANALYSIS].[TRADE_DATE] AS LDATE,
        ''' 
      #Instruct SQL command 
      sqlCmd = '''
        SELECT
          [SECURITY_ANALYSIS].[INNER_CODE] AS INNER_CODE,
          [SECURITY_ANALYSIS].[LCLOSE] AS LCLOSE,
          [SECURITY_ANALYSIS].[TOPEN] AS TOPEN,
          [SECURITY_ANALYSIS].[TCLOSE] AS TCLOSE,
          [SECURITY_ANALYSIS].[THIGH] AS THIGH,
          [SECURITY_ANALYSIS].[TLOW] AS TLOW,
          [SECURITY_ANALYSIS].[TVOLUME] AS TVOLUME,
          [SECURITY_ANALYSIS].[TVALUE] AS TVALUE,
          [SECURITY_ANALYSIS].[CHNG] AS CHNG,
          [SECURITY_ANALYSIS].[EXCHR] AS EXCHR,
          ''' + field + '''
          ''' + dbDict[db]['TRADEDATE'] + '''
        FROM
          ''' + dbDict[db]['TABLE']['ANA'] + ''' AS [SECURITY_ANALYSIS]
        WHERE
          ''' + condition + '''
        ORDER BY
          INNER_CODE,
          ''' + dbDict[db]['ORDER']
      records = self.mssqlHandle.sqlQuery(sqlCmd)
      #records = self.mssqlHandle.sqlQueryProc('p_list_all', (db,))
      logFile.logInfo('Get analysis info succeed.')

      innerCode = ''
      #securityInfoDict = {} 内存较大适用
      #单个证券多日衍生数据列表
      securityInfoList = list()
      #遍历查询记录
      for rec in records:
        infoDict = {}
        #遍历基础信息字段
        for column in dbDict[db]['COLUMN']['ANA']:
          #查询结果以字段名称为key值存入有序字典
          infoDict[column] = rec[column]
        #if ((db == 'WEEK') or (db == 'MONTH')):
        infoDict['FDATE'] = rec['FDATE']
        infoDict['LDATE'] = rec['LDATE']
        #不同股票写入不同字典键值中
        if ((rec['INNER_CODE'] != innerCode) and (innerCode != '')):
          
          queue.put({'DB':db,'MSG':securityInfoList})
          #securityInfoDict[innerCode] = securityInfoList 内存较大适用
          securityInfoList = list()
          innerCode = rec['INNER_CODE']
        elif (innerCode == ''):
          innerCode = rec['INNER_CODE']
        #将有序字典追加到结果集列表中
        securityInfoList.append(infoDict)
      else:
        #securityInfoDict[innerCode] = securityInfoList 内存较大适用
        queue.put({'DB':db,'MSG':securityInfoList})
        #securityInfoList = list()
      
      #for innerCode in securityInfoDict: 内存较大适用
      #  queue.put({'DB':db,'MSG':securityInfoDict[innerCode]})
      logFile.logInfo('Standardize analysis info succeed.')
    except IndexError:
      logFile.logInfo('Invalid table name.')
      print 'Invalid table name.'
      