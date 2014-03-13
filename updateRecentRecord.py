# -*- coding: UTF-8 -*-
'''
Created on 2013-12-19

@author: Grayson
'''
#system
from collections import OrderedDict
from datetime import datetime
from decimal import Decimal
import sys
#user
from baseObject import BaseObject
from constant import dbDict, constantDict
from common import equalZero, datetime2days
from splitSecurity import SplitSecurity
from excludeRightDividend import ExcludeRightDividend
import sys, os
sys.path.append(os.getcwd() + r'\..\Common')
from mssqlAPI import MssqlAPI
from mongoAPI import MongoAPI


def GetRecentAnalysisRecords(db, mssqlHandle):
  '''
  Get most recent 135 records for every securities from analysis and extension table.
  '''
  print '>>', sys._getframe().f_code.co_name
  procDict = {'DAY':'p_GetRecentDayTradingInfo', 'WEEK':'p_GetRecentWeekTradingInfo', 'MONTH':'p_GetRecentMonthTradingInfo'}
  records = mssqlHandle.sqlQueryProc(procDict[db], ())
  securityInfoDict = {}
  securityInfoList = []
  innerCode = ''
  #遍历查询记录
  for rec in records:
    infoDict = {}
    #遍历基础信息字段
    for column in dbDict[db]['COLUMN']['ANA']:
      #基本信息查询结果以字段名称为key值存入字典
      infoDict[column] = rec[column]
    if ((db == 'WEEK') or (db == 'MONTH')):
      infoDict['FDATE'] = rec['FDATE']
    for column in dbDict[db]['COLUMN']['EXT']:
      #衍生信息查询结果以字段名称为key值存入字典
      infoDict[column] = rec[column]
    #不同股票写入不同字典键值中
    if ((rec['INNER_CODE'] != innerCode) and (innerCode != '')):
      securityInfoDict[innerCode] = securityInfoList
      securityInfoList = []
      innerCode = rec['INNER_CODE']
    elif (innerCode == ''):
      innerCode = rec['INNER_CODE']
    #将单条记录字典追加到结果集列表中
    securityInfoList.append(infoDict)
  else:
    securityInfoDict[innerCode] = securityInfoList
    securityInfoList = []
    del mssqlHandle
  print '<<', sys._getframe().f_code.co_name
  return securityInfoDict


def GetTradeNum(mssqlHandle):
  '''
  Get day/week/month of trading records number
  '''
  print '>>', sys._getframe().f_code.co_name
  records = mssqlHandle.sqlQueryProc('p_GetTradeNum', ())
  tradeNumDict = {}
  for rec in records:
    tradeNumDict[rec['INNER_CODE']] = {'DAY':rec['DAYS'], 'WEEK':rec['WEEKS'], 'MONTH':rec['MONTHS']}
  print '<<', sys._getframe().f_code.co_name
  return tradeNumDict


def CalculateRecentExtensionInfo(db, securityInfoDict, tradeNumDict, excludeRightDividendDict):
  '''
  Calculate extend history info.
  #计算指定证券最新一日衍生数据
  '''
  print '>>', sys._getframe().f_code.co_name
  #收盘价列表
  try:
    closeTuple = (9, 34)
    highDict = OrderedDict()
    lowDict = OrderedDict()
    for close in closeTuple:
      highDict[close] = BaseObject(close)
      lowDict[close] = BaseObject(close)
    #收盘价列表
    priceTuple = (5, 10, 13, 14, 20, 25, 43)
    priceDict = OrderedDict()
    for price in priceTuple:
      priceDict[price] = BaseObject(price)
    #成交金额列表
    volumeTuple = (5, 10, 30, 60, 135)
    volumeDict = OrderedDict()
    for volume in volumeTuple:
      volumeDict[volume] = BaseObject(volume)
    #最新记录信息
    recentInfoDict = {}
    #遍历指定股票全部信息
    for innerCode in securityInfoDict:
      extensionInfo = {}
      for analysisInfo in securityInfoDict[innerCode]:
        #衍生记录
        #收盘价列表
        for closeType in closeTuple:
          highDict[closeType].append(analysisInfo['THIGH'])
          lowDict[closeType].append(analysisInfo['TLOW'])
        #收盘价列表
        for priceType in priceTuple:
          priceDict[priceType].append(int(analysisInfo['TCLOSE'] * 65536))
        #成交量列表
        for volumeType in volumeTuple:
          volumeDict[volumeType].append(int(analysisInfo['TVOLUME'] * 65536))
        #成交价格均值
        for priceType in priceTuple:
          sumPrice = int(0)
          for price in priceDict[priceType].object():
            sumPrice += int(price)
          sumPrice = Decimal(sumPrice) / 65536
          extensionInfo['MA' + str(priceType)] = Decimal(sumPrice) / len(priceDict[priceType].object())
        #成交量均值
        for volumeType in volumeTuple:
          sumVolume = int(0)
          for volume in volumeDict[volumeType].object():
            sumVolume += int(volume)
          sumVolume = Decimal(sumVolume) / 65536
          extensionInfo['VOL' + str(volumeType)] = Decimal(sumVolume) / len(volumeDict[volumeType].object())
      else:
        extensionInfo['INNER_CODE'] = analysisInfo['INNER_CODE']
        if (db == 'DAY'):
          extensionInfo['TRADE_DATE'] = analysisInfo['TRADE_DATE']
          #除权除息信息
          try:
            #交易日恰为除权除息日 或 交易日晚于除权除息日且前一交易日早于除权除息日，则判断当前交易日为除权除息状态
            if ((analysisInfo['TRADE_DATE'] == excludeRightDividendDict[innerCode]['date']) or
                ((securityInfoDict[innerCode][len(securityInfoDict[innerCode])-2]['TRADE_DATE'] < excludeRightDividendDict[innerCode]['date']) and
                 ((analysisInfo['TRADE_DATE'] > excludeRightDividendDict[innerCode]['date'])))):
              extensionInfo['XRXD'] = excludeRightDividendDict[innerCode]['type']
            else:
              extensionInfo['XRXD'] = 3
          except IndexError:
            extensionInfo['XRXD'] = 3
        elif (db == 'WEEK'):
          extensionInfo['TRADE_YEAR'] = analysisInfo['TRADE_YEAR']
          extensionInfo['TRADE_WEEK'] = analysisInfo['TRADE_WEEK']
          #除权除息信息
          try:
            #交易日恰为除权除息日 或 交易日晚于除权除息日且前一交易日早于除权除息日，则判断当前交易日为除权除息状态
            if ((analysisInfo['LAST_TRADE_DATE'] == excludeRightDividendDict[innerCode]['date']) or
                ((securityInfoDict[innerCode][len(securityInfoDict[innerCode])-2]['LAST_TRADE_DATE'] < excludeRightDividendDict[innerCode]['date']) and
                 ((analysisInfo['LAST_TRADE_DATE'] > excludeRightDividendDict[innerCode]['date'])))):
              extensionInfo['XRXD'] = excludeRightDividendDict[innerCode]['type']
            else:
              extensionInfo['XRXD'] = 3
          except IndexError:
            extensionInfo['XRXD'] = 3
        elif (db == 'MONTH'):
          extensionInfo['TRADE_YEAR'] = analysisInfo['TRADE_YEAR']
          extensionInfo['TRADE_MONTH'] = analysisInfo['TRADE_MONTH']
          #除权除息信息
          try:
            #交易日恰为除权除息日 或 交易日晚于除权除息日且前一交易日早于除权除息日，则判断当前交易日为除权除息状态
            if ((analysisInfo['LAST_TRADE_DATE'] == excludeRightDividendDict[innerCode]['date']) or
                ((securityInfoDict[innerCode][len(securityInfoDict[innerCode])-2]['LAST_TRADE_DATE'] < excludeRightDividendDict[innerCode]['date']) and
                 ((analysisInfo['LAST_TRADE_DATE'] > excludeRightDividendDict[innerCode]['date'])))):
              extensionInfo['XRXD'] = excludeRightDividendDict[innerCode]['type']
            else:
              extensionInfo['XRXD'] = 3
          except IndexError:
            extensionInfo['XRXD'] = 3
        #KDJ值
        for kdj in constantDict['KDJ']:
          if (tradeNumDict[innerCode] <= 1):
            if (equalZero(analysisInfo['THIGH'] - analysisInfo['TLOW']) == True):
              extensionInfo[constantDict['KDJ'][kdj]['NAME']['RSV']] = constantDict['ZERO']
              extensionInfo[constantDict['KDJ'][kdj]['NAME']['K']] = constantDict['ZERO']
              extensionInfo[constantDict['KDJ'][kdj]['NAME']['D']] = constantDict['ZERO']
              extensionInfo[constantDict['KDJ'][kdj]['NAME']['J']] = constantDict['ZERO']
            else:
              extensionInfo[constantDict['KDJ'][kdj]['NAME']['RSV']] = (analysisInfo['TCLOSE'] - analysisInfo['TLOW']) / (analysisInfo['THIGH'] - analysisInfo['TLOW']) * 100
              extensionInfo[constantDict['KDJ'][kdj]['NAME']['K']] = extensionInfo[constantDict['KDJ'][kdj]['NAME']['RSV']]
              extensionInfo[constantDict['KDJ'][kdj]['NAME']['D']] = extensionInfo[constantDict['KDJ'][kdj]['NAME']['RSV']]
              extensionInfo[constantDict['KDJ'][kdj]['NAME']['J']] = extensionInfo[constantDict['KDJ'][kdj]['NAME']['RSV']]
          else:
            extensionInfo[constantDict['KDJ'][kdj]['NAME']['RSV']] = \
              Decimal(0) if (equalZero(highDict[kdj[0]].max() - lowDict[kdj[0]].min()) == True) \
              else ((analysisInfo['TCLOSE'] - lowDict[kdj[0]].min()) / (highDict[kdj[0]].max() - lowDict[kdj[0]].min()) * 100)
            extensionInfo[constantDict['KDJ'][kdj]['NAME']['K']] = \
              constantDict['KDJ'][kdj]['PARA']['KP1'] * securityInfoDict[innerCode][len(securityInfoDict[innerCode])-2][constantDict['KDJ'][kdj]['NAME']['K']] + \
              constantDict['KDJ'][kdj]['PARA']['KP2'] * extensionInfo[constantDict['KDJ'][kdj]['NAME']['RSV']] 
            extensionInfo[constantDict['KDJ'][kdj]['NAME']['D']] = \
              constantDict['KDJ'][kdj]['PARA']['DP1'] * securityInfoDict[innerCode][len(securityInfoDict[innerCode])-2][constantDict['KDJ'][kdj]['NAME']['D']] + \
              constantDict['KDJ'][kdj]['PARA']['DP2'] * extensionInfo[constantDict['KDJ'][kdj]['NAME']['K']] 
            extensionInfo[constantDict['KDJ'][kdj]['NAME']['J']] = \
              3 * extensionInfo[constantDict['KDJ'][kdj]['NAME']['K']] - \
              2 * extensionInfo[constantDict['KDJ'][kdj]['NAME']['D']]
          
        #EMA值
        for ema in constantDict['EMA']:
          if (tradeNumDict[innerCode] > 2):
            extensionInfo[constantDict['EMA'][ema]['NAME']] = \
              constantDict['EMA'][ema]['PARA']['P1'] * analysisInfo['TCLOSE'] + \
              constantDict['EMA'][ema]['PARA']['P2'] * securityInfoDict[innerCode][len(securityInfoDict[innerCode])-2][constantDict['EMA'][ema]['NAME']]
          elif (tradeNumDict[innerCode] == 2):
            extensionInfo[constantDict['EMA'][ema]['NAME']] = \
              constantDict['EMA'][ema]['PARA']['P1'] * analysisInfo['TCLOSE'] + \
              constantDict['EMA'][ema]['PARA']['P2'] * analysisInfo['LCLOSE']
          else:
            extensionInfo[constantDict['EMA'][ema]['NAME']] = analysisInfo['TCLOSE']
            
        #MACD值
        for macd in constantDict['MACD']:
          if (tradeNumDict[innerCode] > 1):
            extensionInfo[constantDict['MACD'][macd]['NAME']['DIF']] = \
              extensionInfo['EMA'+str(macd[0])] - extensionInfo['EMA'+str(macd[1])]
            extensionInfo[constantDict['MACD'][macd]['NAME']['DEA']] = \
              constantDict['MACD'][macd]['PARA']['P1'] * extensionInfo[constantDict['MACD'][macd]['NAME']['DIF']] + \
              constantDict['MACD'][macd]['PARA']['P2'] * securityInfoDict[innerCode][len(securityInfoDict[innerCode])-2][constantDict['MACD'][macd]['NAME']['DEA']]
            extensionInfo[constantDict['MACD'][macd]['NAME']['MACD']] = \
              (extensionInfo[constantDict['MACD'][macd]['NAME']['DIF']] - extensionInfo[constantDict['MACD'][macd]['NAME']['DEA']]) * 2
          else:
            extensionInfo[constantDict['MACD'][macd]['NAME']['DIF']] = constantDict['ZERO']
            extensionInfo[constantDict['MACD'][macd]['NAME']['DEA']] = constantDict['ZERO']
            extensionInfo[constantDict['MACD'][macd]['NAME']['MACD']] = constantDict['ZERO']
          
      #最新记录存入字典
      recentInfoDict[innerCode] = extensionInfo
      print 'innerCode:', innerCode
      print extensionInfo
      print
      
    print '<<', sys._getframe().f_code.co_name
    return recentInfoDict 
    
  except Exception,e:
    print e


def InsertRecentIntoSQL(mssqlHandle, db, recentInfoDict):
  print '>>', sys._getframe().f_code.co_name
  for innerCode in recentInfoDict:
    columnStr = ''
    valueStr = ''
    for key in dbDict[db]['COLUMN']['EXT']:
      columnStr += key + ','
      if (key in ('MA5', 'MA10', 'MA13', 'MA14', 'MA20', 'MA25', 'MA43', 'VOL5', 'VOL10', 'VOL30', 'VOL60', 'VOL135')):
        valueStr += 'convert(numeric(18,4),' + str(recentInfoDict[innerCode][key]) + '),'
      elif (key in ('RSV9', 'K9', 'D3', 'J3', 'RSV34', 'K34', 'D9', 'J9', 'EMA5', 'EMA6', 'EMA12', 'EMA13', 'EMA26', 'EMA35', \
                    'DIF1', 'DEA1', 'MACD1', 'DIF2', 'DEA2', 'MACD2', 'DIF3', 'DEA3', 'MACD3')):
        valueStr += 'convert(numeric(9,4),' + str(recentInfoDict[innerCode][key]) + '),'
      else:
        valueStr += "'" + str(recentInfoDict[innerCode][key]) + "',"
    columnStr = columnStr[0:-1]
    valueStr = valueStr[0:-1]
    sqlCmd = '''
      INSERT INTO
      ''' + dbDict[db]['TABLE']['EXT'] + '''
      (''' + columnStr + ''')
      VALUES
      ('''+ valueStr +''');
      '''
    #mssqlHandle.sqlAppend(sqlCmd)
  print '<<', sys._getframe().f_code.co_name
  return None
    
    
def InsertRecentIntoMongo(mongoHandle, db, recentInfoDict, splitSecurityDict):
  print '>>', sys._getframe().f_code.co_name
  for innerCode in recentInfoDict:
    infoDict = OrderedDict()
    idDict = OrderedDict()
    if (db == 'DAY'):
      timeOffset = datetime2days(recentInfoDict[innerCode]['TRADE_DATE'])
      idDict['IC'] = int(recentInfoDict[innerCode]['INNER_CODE'])
      idDict['TO'] = timeOffset
      infoDict['_id'] = idDict
      infoDict['TO'] = timeOffset
    elif (db == 'WEEK'):
      idDict['IC'] = int(recentInfoDict[innerCode]['INNER_CODE'])
      idDict['YR'] = recentInfoDict[innerCode]['TRADE_YEAR']
      idDict['WK'] = recentInfoDict[innerCode]['TRADE_WEEK']
      infoDict['_id'] = idDict
      infoDict['TO'] = datetime2days(recentInfoDict[innerCode]['LAST_TRADE_DATE'])
    elif (db == 'MONTH'):
      idDict['IC'] = int(recentInfoDict[innerCode]['INNER_CODE'])
      idDict['YR'] = recentInfoDict[innerCode]['TRADE_YEAR']
      idDict['MN'] = recentInfoDict[innerCode]['TRADE_MONTH']
      infoDict['_id'] = idDict
      infoDict['TO'] = datetime2days(recentInfoDict[innerCode]['LAST_TRADE_DATE'])
    infoDict['OP'] = float(recentInfoDict[innerCode]['TOPEN'])
    infoDict['CP'] = float(recentInfoDict[innerCode]['TCLOSE'])
    infoDict['HP'] = float(recentInfoDict[innerCode]['THIGH'])
    infoDict['LP'] = float(recentInfoDict[innerCode]['TLOW'])
    infoDict['VL'] = long(recentInfoDict[innerCode]['TVOLUME'])
    infoDict['AM'] = float(recentInfoDict[innerCode]['TVALUE'])
    infoDict['TR'] = float(0.0)
    infoDict['CA'] = float(recentInfoDict[innerCode]['CHNG'])
    infoDict['AA'] = [float(recentInfoDict[innerCode]['MA5']), float(recentInfoDict[innerCode]['MA10']), float(recentInfoDict[innerCode]['MA13']), 
                      float(recentInfoDict[innerCode]['MA14']), float(recentInfoDict[innerCode]['MA20']),float(recentInfoDict[innerCode]['MA25']), 
                      float(recentInfoDict[innerCode]['MA43'])]
    infoDict['AV'] = [long(recentInfoDict[innerCode]['VOL5']), long(recentInfoDict[innerCode]['VOL10']), long(recentInfoDict[innerCode]['VOL30']), 
                      long(recentInfoDict[innerCode]['VOL60']), long(recentInfoDict[innerCode]['VOL135'])]
    infoDict['KV'] = [float(recentInfoDict[innerCode]['K9']), float(recentInfoDict[innerCode]['K34'])] 
    infoDict['DV'] = [float(recentInfoDict[innerCode]['D3']), float(recentInfoDict[innerCode]['D9'])]
    infoDict['JV'] = [float(recentInfoDict[innerCode]['J3']), float(recentInfoDict[innerCode]['J9'])]
    infoDict['DIF'] = [float(recentInfoDict[innerCode]['DIF1']), float(recentInfoDict[innerCode]['DIF2']), float(recentInfoDict[innerCode]['DIF3'])]
    infoDict['DEA'] = [float(recentInfoDict[innerCode]['DEA1']), float(recentInfoDict[innerCode]['DEA2']), float(recentInfoDict[innerCode]['DEA3'])]
    infoDict['MACD'] = [float(recentInfoDict[innerCode]['MACD1']), float(recentInfoDict[innerCode]['MACD2']), float(recentInfoDict[innerCode]['MACD3'])]
    infoDict['ER'] = int(recentInfoDict[innerCode]['XRXD'])
    infoDict['RSV'] = [float(recentInfoDict[innerCode]['RSV9']), float(recentInfoDict[innerCode]['RSV34'])]
    infoDict['EMA'] = [float(recentInfoDict[innerCode]['EMA5']), float(recentInfoDict[innerCode]['EMA6']), float(recentInfoDict[innerCode]['EMA12']),  
                       float(recentInfoDict[innerCode]['EMA13']), float(recentInfoDict[innerCode]['EMA26']), float(recentInfoDict[innerCode]['EMA35'])]
    
    mongoHandle.update(dbDict[db]['TABLE']['MONGO'], 'data_'+ splitSecurityDict[db][innerCode], {'_id':idDict}, infoDict)
  print '<<', sys._getframe().f_code.co_name
  return None


#获取分表名信息
def DisposeSplitSecurity(mssqlDict, mongoDIct):
  print '>>', sys._getframe().f_code.co_name
  splitSecurity = SplitSecurity(mssqlDict, mongoDIct)
  splitSecurityDict = {}
  for db in dbDict:
    securityCodeList = splitSecurity.GetSecurityCodeList(db)
    splitSecurityDict[db] = splitSecurity.SplitSecurityIndex(securityCodeList, dbDict[db]['SPLIT'])
  print '<<', sys._getframe().f_code.co_name
  return splitSecurityDict


if __name__ == '__main__':
  startTime = datetime.now()
  print 'Start at:',startTime
  
  mssqlDict = {'SERVER':'115.236.23.251,1433', 'DB':'QN_Quotation', 'USER':'dingguangbo', 'PWD':'9bc@#Knb1jB*@KNDIH4'}
  mongoDict = {'SERVER':'10.0.18.23','PORT':27017}
  
  mssqlHandle = MssqlAPI(server = mssqlDict['SERVER'], db = mssqlDict['DB'], user = mssqlDict['USER'], pwd = mssqlDict['PWD'])
  mongoHandle = MongoAPI(server = mongoDict['SERVER'], port = mongoDict['PORT'])
  excludeRightDividen = ExcludeRightDividend(server = mssqlDict['SERVER'], db = mssqlDict['DB'], user = mssqlDict['USER'], pwd = mssqlDict['PWD'])
  excludeRightDividendDict = excludeRightDividen.GetRecentExcludeRightDividenInfo()
  splitSecurityDict = DisposeSplitSecurity(mssqlDict, mongoDict)
  
  #获取各股交易周期数量
  tradeNumDict = GetTradeNum(mssqlHandle)
  for db in dbDict:
    #获取最新行情信息
    securityInfoDict = GetRecentAnalysisRecords(db, mssqlHandle)
    #计算各股最近交易日数据
    recentInfoDict = CalculateRecentExtensionInfo(db, securityInfoDict, tradeNumDict, excludeRightDividendDict)
    #MSSQL写入
    InsertRecentIntoSQL(mssqlHandle, db, recentInfoDict)
    #MongoDB写入
    InsertRecentIntoMongo(mongoHandle, db, recentInfoDict, splitSecurityDict)
  
  finalTime = datetime.now()
  deltaTime = finalTime - startTime
  totalTime = deltaTime.total_seconds()
  totalHour = totalTime // 3600
  totalMin = (totalTime % 3600) // 60
  totalSec = totalTime % 60
  print("Total time: %d(h)%d(m)%d(s)" % (totalHour, totalMin, totalSec))