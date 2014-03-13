# -*- coding: UTF-8 -*-
'''
Created on 2013-12-10

@author: Grayson
'''
from common   import rightType
import sys, os
sys.path.append(os.getcwd() + r'\..\Common')
from mssqlAPI import MssqlAPI



class ExcludeRightDividend(object):
  '''
  classdocs
  '''


  def __init__(self, server, db, user, pwd):
    '''
    Constructor
    '''
    self.mssqlHandle = MssqlAPI(server, db, user, pwd)
    
    
  def __del__(self):
    del self.mssqlHandle
    
  
  def GetExcludeRightDividendInfo(self):
    sqlCmd = self.__ExcludeRightDividendSqlCmd()
    records = self.mssqlHandle.sqlQuery(sqlCmd)
    excludeRightDividendDict = dict()
    excludeRightDividendList = list()
    innerCode = ''
    recDict = dict()
    for rec in records:
      #同一证券以列表方式按时间升序存放到以证券代码为键值的字典中
      recDict['date'] = rec['DATE']
      recDict['type'] = rightType(rec['CASH_BT'], rec['CIRCULATION_CHANGE_RATIO']) 
      if ((rec['INNER_CODE'] != innerCode) and (innerCode != '')):
        excludeRightDividendDict[innerCode] = excludeRightDividendList
        excludeRightDividendList = list()
        innerCode = rec['INNER_CODE']
      elif (innerCode == ''):
        innerCode = rec['INNER_CODE']
      excludeRightDividendList.append(recDict.copy())
      recDict.clear()
    else:
      excludeRightDividendDict[innerCode] = excludeRightDividendList

    return excludeRightDividendDict
  
  
  def GetRecentExcludeRightDividendInfo(self):
    sqlCmd = self.__ExcludeRightDividendSqlCmd()
    records = self.mssqlHandle.sqlQuery(sqlCmd)
    excludeRightDividendDict = dict()
    innerCode = ''
    recDict = dict()
    excludeRightDividend = dict()
    for rec in records:
      #同一证券最新除权除息信息存放到以证券代码为键值的字典中

      recDict['date'] = rec['DATE']
      recDict['type'] = rightType(rec['CASH_BT'], rec['CIRCULATION_CHANGE_RATIO']) 
      if ((rec['INNER_CODE'] != innerCode) and (innerCode != '')):
        excludeRightDividendDict[innerCode] = excludeRightDividend.copy()
        excludeRightDividend.clear()
        innerCode = rec['INNER_CODE']
      elif (innerCode == ''):
        innerCode = rec['INNER_CODE']
      excludeRightDividend = recDict.copy()
      recDict.clear()
    else:
      excludeRightDividendDict[innerCode] = excludeRightDividend.copy()

    return excludeRightDividendDict
  
 
  def __ExcludeRightDividendSqlCmd(self):
    '''
    SELECT * FROM (
        SELECT
          RTRIM(
          CASE
            WHEN [PUB_SEC_CODE].[SEC_TYPE] = '5' THEN
              CASE
                WHEN [PUB_SEC_CODE].[MKT_TYPE] = '2' THEN '21' + [PUB_SEC_CODE].[SEC_CODE]
                WHEN [PUB_SEC_CODE].[MKT_TYPE] = '1' THEN '22' + [PUB_SEC_CODE].[SEC_CODE]
              END
            WHEN [PUB_SEC_CODE].[SEC_TYPE] = '1' THEN
              CASE
                WHEN [PUB_SEC_CODE].[MKT_TYPE] = '2' THEN '11' + [PUB_SEC_CODE].[SEC_CODE]
                WHEN [PUB_SEC_CODE].[MKT_TYPE] = '1' THEN '12' + [PUB_SEC_CODE].[SEC_CODE]
              END
          END) AS INNER_CODE,
          [STK_ALLOT_RESULT].[A_EXRGT_DATE] AS [DATE],
          0 AS [CASH_BT],
          [STK_ALLOT_RESULT].[ALLOT_PCT] AS [ALLOT_PCT],
          [STK_ALLOT_RESULT].[ALLOT_PRC] AS [ALLOT_PRC],
          0 AS CIRCULATION_CHANGE_RATIO
        FROM
          [Genius].[dbo].[STK_ALLOT_RESULT] AS [STK_ALLOT_RESULT],
          [Genius].[dbo].[PUB_SEC_CODE] AS [PUB_SEC_CODE]
        WHERE
          [STK_ALLOT_RESULT].[COMCODE] = [PUB_SEC_CODE].[COMCODE] AND
          [STK_ALLOT_RESULT].[A_EXRGT_DATE] IS NOT NULL AND
          [PUB_SEC_CODE].[SEC_STYPE] IN ('101', '501') AND
          [PUB_SEC_CODE].[SEC_CODE] IS NOT NULL AND
          [PUB_SEC_CODE].[SEC_SNAME] IS NOT NULL AND
          [PUB_SEC_CODE].[MKT_TYPE] IS NOT NULL

        UNION ALL

        SELECT
          RTRIM(
          CASE
            WHEN [PUB_SEC_CODE].[SEC_TYPE] = '5' THEN
              CASE
                WHEN [PUB_SEC_CODE].[MKT_TYPE] = '2' THEN '21' + [PUB_SEC_CODE].[SEC_CODE]
                WHEN [PUB_SEC_CODE].[MKT_TYPE] = '1' THEN '22' + [PUB_SEC_CODE].[SEC_CODE]
              END
            WHEN [PUB_SEC_CODE].[SEC_TYPE] = '1' THEN
              CASE
                WHEN [PUB_SEC_CODE].[MKT_TYPE] = '2' THEN '11' + [PUB_SEC_CODE].[SEC_CODE]
                WHEN [PUB_SEC_CODE].[MKT_TYPE] = '1' THEN '12' + [PUB_SEC_CODE].[SEC_CODE]
              END
          END) AS INNER_CODE,
          [STK_DIV_INFO].[EX_DIVI_DATE] AS DATE,
          CASE
            WHEN [STK_DIV_INFO].[CASH_BT] IS NULL THEN 0
            ELSE [STK_DIV_INFO].[CASH_BT]
          END AS [CASH_BT],
          0 AS [ALLOT_PCT],
          0 AS [ALLOT_PRC],
          CASE
            WHEN [STK_DIV_INFO].[BONUS_SHR] IS NULL THEN
              CASE
                WHEN [STK_DIV_INFO].[CAP_SHR] IS NULL THEN 0
                ELSE [STK_DIV_INFO].[CAP_SHR] / 10
              END
            ELSE [STK_DIV_INFO].[BONUS_SHR] / 10
          END AS CIRCULATION_CHANGE_RATIO
        FROM
          [Genius].[dbo].[STK_DIV_INFO] AS [STK_DIV_INFO],
          [Genius].[dbo].[PUB_SEC_CODE] AS [PUB_SEC_CODE]
        WHERE
          [STK_DIV_INFO].[COMCODE] = [PUB_SEC_CODE].[COMCODE] AND
          [STK_DIV_INFO].[EX_DIVI_DATE] IS NOT NULL AND
          [PUB_SEC_CODE].[SEC_STYPE] IN ('101', '501') AND
          [PUB_SEC_CODE].[SEC_CODE] IS NOT NULL AND
          [PUB_SEC_CODE].[SEC_SNAME] IS NOT NULL AND
          [PUB_SEC_CODE].[MKT_TYPE] IS NOT NULL AND
          [STK_DIV_INFO].[PRG_CODE] = '29' AND
          [STK_DIV_INFO].[STK_TYPE_CODE] = '1'
        ) t
      ORDER BY
        [t].[INNER_CODE],
        [t].[DATE]
    '''
    sqlCmd = '''
      SELECT
        [INNER_CODE],
        [EX_DIVI_DATE] as DATE,
        [CASH_BT],
        [ALLOT_PCT],
        [ALLOT_PRC],
        CASE
          WHEN [CAP_SHR] IS NULL THEN
            CASE
              WHEN [ALLOT_PCT] IS NULL THEN
                CASE
                  WHEN [BONUS_SHR] IS NULL THEN 0
                  ELSE [BONUS_SHR] / 10
                END
              ELSE
                CASE
                  WHEN [BONUS_SHR] IS NULL THEN [ALLOT_PCT] / 10
                  ELSE ([ALLOT_PCT] + [BONUS_SHR]) / 10
                END
            END
          ELSE
            CASE
              WHEN [ALLOT_PCT] IS NULL THEN
                CASE
                  WHEN [BONUS_SHR] IS NULL THEN [CAP_SHR] / 10
                  ELSE ([CAP_SHR] + [BONUS_SHR]) / 10
                END
              ELSE
                CASE
                  WHEN [BONUS_SHR] IS NULL THEN ([CAP_SHR] + [ALLOT_PCT]) / 10
                  ELSE ([CAP_SHR] + [ALLOT_PCT] + [BONUS_SHR]) / 10
                END
            END
        END AS CIRCULATION_CHANGE_RATIO
      FROM
        [QN_Quotation].[dbo].[QN_DIV_INFO]
      ORDER BY
        INNER_CODE,
        DATE
      '''
    return sqlCmd
  
  
if __name__ == '__main__':
  excludeRightDividend = ExcludeRightDividend(server='115.236.23.251,1433', db='QN_Quotation', user='dingguangbo', pwd='9bc@#Knb1jB*@KNDIH4')
  excludeRightDividendDict = excludeRightDividend.GetExcludeRightDividendInfo()
  print excludeRightDividendDict
    