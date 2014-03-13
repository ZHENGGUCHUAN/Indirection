# -*- coding: UTF-8 -*-
'''
Created on 2013-12-06

@author: Grayson
'''
from collections  import OrderedDict
from baseObject   import BaseObject
from decimal      import Decimal
from common       import equalZero
from constant     import constantDict
from datetime     import datetime
import splitSecurity
import sys, os
sys.path.append(os.getcwd() + r'\..\Common')
from dbServer import mssqlDbServer, mongoDbServer
import mongoAPI, logFile


def CalculateExtensionInfo(db, securityInfoList, excludeRightDividendList, securityInfoQueue):
  '''
  Calculate extend history info.
  #计算指定证券衍生信息数据
  '''
  #print '>> CalculateExtensionInfo'
  try:
    #中间库数据及计算衍生数据与mongo库数据对照比较
    '''
    我也不想把这段代码嵌在计算函数里面，可是现在使用数据队列的方式，每计算一条记录就抛入队列用于写入操作，所以只有在这里，单个股票数据是最完整的。

    #获取各股票分表信息
    splitDict = {'DAY':200, 'WEEK':100, 'MONTH':50}
    splitSecurityIndex = splitSecurity.SplitSecurity(mssqlDbServer['251']['LAN'], ())
    securityCodeList = splitSecurityIndex.GetSecurityCodeList(db)
    splitIndexDict = splitSecurityIndex.SplitSecurityIndex(securityCodeList, splitDict[db])
    #从Mongo获取指定证券全部K线信息
    mongoHandle = mongoAPI.MongoAPI(server=mongoDbServer['235']['LAN']['SERVER'], port=mongoDbServer['235']['LAN']['PORT'])
    if (db == 'DAY'):
      dbName = 'KDayData'
    elif (db == 'WEEK'):
      dbName = 'KWeeklyData'
    elif (db == 'MONTH'):
      dbName = 'KMonthData'
    else:
      raise Exception('Invalid db type.')
    innerCode = splitIndexDict[securityInfoList[0]['INNER_CODE']]
    colName = 'data_' + splitIndexDict[str(innerCode)]
    #Mongo查询结果集
    queryResult = mongoHandle.find(database=dbName, collection=colName, spec={'_id.IC':innerCode}, sort={'TO':1})
    logHandle = logFile.LogFile(name='DataComparison')
    if (len(securityInfoList) != len(queryResult)):
      #SQL数据记录条数与mongo比较
      logStr = '[' + str(innerCode) + ']SQL(' + str(len(securityInfoList)) + ') != ' + 'Mongo(' + str(len(queryResult)) + ')'
      logHandle.logInfo(logStr)
    '''
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
    #除权除息日
    noneExcludeInfo = {'date':datetime(1970,1,1), 'type':3}
    if (len(excludeRightDividendList) > 0):
      excludeInfo = excludeRightDividendList[0]
    else:
      excludeInfo = noneExcludeInfo
    #现有记录数量
    recordNum = 0
    recentExtensionInfo = dict()
    #日周月合并表导入最多61条记录信息
    totalRecordNum = len(securityInfoList)
    #遍历指定股票全部信息
    for analysisInfo in securityInfoList:
      if (totalRecordNum - recordNum <= 61):
        writeMergeTable = True
      else:
        writeMergeTable = False
      #衍生记录
      extensionInfo = OrderedDict()
      extensionInfo['INNER_CODE'] = analysisInfo['INNER_CODE']
      excludeType = 3
      if (db == 'DAY'):
        extensionInfo['TRADE_DATE'] = analysisInfo['TRADE_DATE']
        #除权除息信息
        if ((analysisInfo['TRADE_DATE'] >= excludeInfo['date']) and (excludeInfo['date'] != noneExcludeInfo['date'])):
          excludeType = excludeInfo['type']
          try:
            excludeInfo = excludeRightDividendList[excludeRightDividendList.index(excludeInfo)+1]
          except IndexError:
            excludeInfo = noneExcludeInfo
        else:
          excludeType = 3
      elif (db == 'WEEK'):
        extensionInfo['TRADE_YEAR'] = analysisInfo['TRADE_YEAR']
        extensionInfo['TRADE_WEEK'] = analysisInfo['TRADE_WEEK']
        #除权除息信息
        if ((analysisInfo['FDATE'] >= excludeInfo['date']) and (excludeInfo['date'] != noneExcludeInfo['date'])):
          excludeType = excludeInfo['type']
          try:
            excludeInfo = excludeRightDividendList[excludeRightDividendList.index(excludeInfo)+1]
          except IndexError:
            excludeInfo = noneExcludeInfo
        else:
          excludeType = 3
      elif (db == 'MONTH'):
        extensionInfo['TRADE_YEAR'] = analysisInfo['TRADE_YEAR']
        extensionInfo['TRADE_MONTH'] = analysisInfo['TRADE_MONTH']
        #除权除息信息
        if ((analysisInfo['FDATE'] >= excludeInfo['date']) and (excludeInfo['date'] != noneExcludeInfo['date'])):
          excludeType = excludeInfo['type']
          try:
            excludeInfo = excludeRightDividendList[excludeRightDividendList.index(excludeInfo)+1]
          except IndexError:
            excludeInfo = noneExcludeInfo
        else:
          excludeType = 3
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
      #KDJ值
      for kdj in constantDict['KDJ']:
        if (recordNum < 1):
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
            constantDict['KDJ'][kdj]['PARA']['KP1'] * recentExtensionInfo[constantDict['KDJ'][kdj]['NAME']['K']] + \
            constantDict['KDJ'][kdj]['PARA']['KP2'] * extensionInfo[constantDict['KDJ'][kdj]['NAME']['RSV']] 
          extensionInfo[constantDict['KDJ'][kdj]['NAME']['D']] = \
            constantDict['KDJ'][kdj]['PARA']['DP1'] * recentExtensionInfo[constantDict['KDJ'][kdj]['NAME']['D']] + \
            constantDict['KDJ'][kdj]['PARA']['DP2'] * extensionInfo[constantDict['KDJ'][kdj]['NAME']['K']] 
          extensionInfo[constantDict['KDJ'][kdj]['NAME']['J']] = \
            3 * extensionInfo[constantDict['KDJ'][kdj]['NAME']['K']] - \
            2 * extensionInfo[constantDict['KDJ'][kdj]['NAME']['D']]
        
        recentExtensionInfo[constantDict['KDJ'][kdj]['NAME']['K']] = extensionInfo[constantDict['KDJ'][kdj]['NAME']['K']]
        recentExtensionInfo[constantDict['KDJ'][kdj]['NAME']['D']] = extensionInfo[constantDict['KDJ'][kdj]['NAME']['D']]
      #EMA值
      for ema in constantDict['EMA']:
        if (recordNum >= 2):
          extensionInfo[constantDict['EMA'][ema]['NAME']] = \
            constantDict['EMA'][ema]['PARA']['P1'] * analysisInfo['TCLOSE'] + \
            constantDict['EMA'][ema]['PARA']['P2'] * recentExtensionInfo[constantDict['EMA'][ema]['NAME']]
        elif (recordNum == 1):
          extensionInfo[constantDict['EMA'][ema]['NAME']] = \
            constantDict['EMA'][ema]['PARA']['P1'] * analysisInfo['TCLOSE'] + \
            constantDict['EMA'][ema]['PARA']['P2'] * analysisInfo['LCLOSE']
        else:
          extensionInfo[constantDict['EMA'][ema]['NAME']] = analysisInfo['TCLOSE']
          
        recentExtensionInfo[constantDict['EMA'][ema]['NAME']] = extensionInfo[constantDict['EMA'][ema]['NAME']]
      #MACD值
      for macd in constantDict['MACD']:
        if (recordNum >= 1):
          extensionInfo[constantDict['MACD'][macd]['NAME']['DIF']] = \
            extensionInfo['EMA'+str(macd[0])] - extensionInfo['EMA'+str(macd[1])]
          extensionInfo[constantDict['MACD'][macd]['NAME']['DEA']] = \
            constantDict['MACD'][macd]['PARA']['P1'] * extensionInfo[constantDict['MACD'][macd]['NAME']['DIF']] + \
            constantDict['MACD'][macd]['PARA']['P2'] * recentExtensionInfo[constantDict['MACD'][macd]['NAME']['DEA']]
          extensionInfo[constantDict['MACD'][macd]['NAME']['MACD']] = \
            (extensionInfo[constantDict['MACD'][macd]['NAME']['DIF']] - extensionInfo[constantDict['MACD'][macd]['NAME']['DEA']]) * 2
        else:
          extensionInfo[constantDict['MACD'][macd]['NAME']['DIF']] = constantDict['ZERO']
          extensionInfo[constantDict['MACD'][macd]['NAME']['DEA']] = constantDict['ZERO']
          extensionInfo[constantDict['MACD'][macd]['NAME']['MACD']] = constantDict['ZERO']
        
        recentExtensionInfo[constantDict['MACD'][macd]['NAME']['DEA']] = extensionInfo[constantDict['MACD'][macd]['NAME']['DEA']]
      #记录数量
      recordNum += 1
      #数据比对
      #record = queryResult.next()

      #基本数据及衍生数据写入队列
      securityInfoQueue.put({'DB':db,'ANA':analysisInfo,'EXT':extensionInfo, 'XRXD':excludeType, 'WMT':writeMergeTable})
      #print 'put:',extensionInfo
    #print '<< CalculateExtensionInfo'
  except Exception,e:
    print e
  return None


def CalculateRecentExtensionInfo(db, securityInfoList, excludeRightDividendList, securityInfoQueue):
  '''
  Calculate extend history info.
  #计算指定证券最新一日衍生数据
  '''
  #print '>> CalculateExtensionInfo'
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
    #除权除息日
    noneExcludeInfo = {'date':datetime(1970,1,1), 'type':3}
    if (len(excludeRightDividendList) > 0):
      excludeInfo = excludeRightDividendList[0]
    else:
      excludeInfo = noneExcludeInfo
    #现有记录数量
    recordNum = 0
    recentExtensionInfo = {}
    extensionInfoDict = OrderedDict()
    analysisInfoDict = OrderedDict()
    excludeType = 3
    #遍历指定股票全部信息
    for analysisInfo in securityInfoList:
      #衍生记录
      extensionInfo = OrderedDict()
      extensionInfo['INNER_CODE'] = analysisInfo['INNER_CODE']
      if (db == 'DAY'):
        extensionInfo['TRADE_DATE'] = analysisInfo['TRADE_DATE']
        #除权除息信息
        if ((analysisInfo['TRADE_DATE'] >= excludeInfo['date']) and (excludeInfo['date'] != noneExcludeInfo['date'])):
          excludeType = excludeInfo['type']
          try:
            excludeInfo = excludeRightDividendList[excludeRightDividendList.index(excludeInfo)+1]
          except IndexError:
            excludeInfo = noneExcludeInfo
        else:
          excludeType = 3
      elif (db == 'WEEK'):
        extensionInfo['TRADE_YEAR'] = analysisInfo['TRADE_YEAR']
        extensionInfo['TRADE_WEEK'] = analysisInfo['TRADE_WEEK']
        #除权除息信息
        if ((analysisInfo['FDATE'] >= excludeInfo['date']) and (analysisInfo['LDATE'] <= excludeInfo['date']) and (excludeInfo['date'] != noneExcludeInfo['date'])):
          excludeType = excludeInfo['type']
          try:
            excludeInfo = excludeRightDividendList[excludeRightDividendList.index(excludeInfo)+1]
          except IndexError:
            excludeInfo = noneExcludeInfo
        else:
          excludeType = 3
      elif (db == 'MONTH'):
        extensionInfo['TRADE_YEAR'] = analysisInfo['TRADE_YEAR']
        extensionInfo['TRADE_MONTH'] = analysisInfo['TRADE_MONTH']
        #除权除息信息
        if ((analysisInfo['FDATE'] >= excludeInfo['date']) and (analysisInfo['LDATE'] <= excludeInfo['date']) and (excludeInfo['date'] != noneExcludeInfo['date'])):
          excludeType = excludeInfo['type']
          try:
            excludeInfo = excludeRightDividendList[excludeRightDividendList.index(excludeInfo)+1]
          except IndexError:
            excludeInfo = noneExcludeInfo
        else:
          excludeType = 3
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
      #KDJ值
      for kdj in constantDict['KDJ']:
        if (recordNum < 1):
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
            constantDict['KDJ'][kdj]['PARA']['KP1'] * recentExtensionInfo[constantDict['KDJ'][kdj]['NAME']['K']] + \
            constantDict['KDJ'][kdj]['PARA']['KP2'] * extensionInfo[constantDict['KDJ'][kdj]['NAME']['RSV']] 
          extensionInfo[constantDict['KDJ'][kdj]['NAME']['D']] = \
            constantDict['KDJ'][kdj]['PARA']['DP1'] * recentExtensionInfo[constantDict['KDJ'][kdj]['NAME']['D']] + \
            constantDict['KDJ'][kdj]['PARA']['DP2'] * extensionInfo[constantDict['KDJ'][kdj]['NAME']['K']] 
          extensionInfo[constantDict['KDJ'][kdj]['NAME']['J']] = \
            3 * extensionInfo[constantDict['KDJ'][kdj]['NAME']['K']] - \
            2 * extensionInfo[constantDict['KDJ'][kdj]['NAME']['D']]
        
        recentExtensionInfo[constantDict['KDJ'][kdj]['NAME']['K']] = extensionInfo[constantDict['KDJ'][kdj]['NAME']['K']]
        recentExtensionInfo[constantDict['KDJ'][kdj]['NAME']['D']] = extensionInfo[constantDict['KDJ'][kdj]['NAME']['D']]
      #EMA值
      for ema in constantDict['EMA']:
        if (recordNum >= 2):
          extensionInfo[constantDict['EMA'][ema]['NAME']] = \
            constantDict['EMA'][ema]['PARA']['P1'] * analysisInfo['TCLOSE'] + \
            constantDict['EMA'][ema]['PARA']['P2'] * recentExtensionInfo[constantDict['EMA'][ema]['NAME']]
        elif (recordNum == 1):
          extensionInfo[constantDict['EMA'][ema]['NAME']] = \
            constantDict['EMA'][ema]['PARA']['P1'] * analysisInfo['TCLOSE'] + \
            constantDict['EMA'][ema]['PARA']['P2'] * analysisInfo['LCLOSE']
        else:
          extensionInfo[constantDict['EMA'][ema]['NAME']] = analysisInfo['TCLOSE']
          
        recentExtensionInfo[constantDict['EMA'][ema]['NAME']] = extensionInfo[constantDict['EMA'][ema]['NAME']]
      #MACD值
      for macd in constantDict['MACD']:
        if (recordNum >= 1):
          extensionInfo[constantDict['MACD'][macd]['NAME']['DIF']] = \
            extensionInfo['EMA'+str(macd[0])] - extensionInfo['EMA'+str(macd[1])]
          extensionInfo[constantDict['MACD'][macd]['NAME']['DEA']] = \
            constantDict['MACD'][macd]['PARA']['P1'] * extensionInfo[constantDict['MACD'][macd]['NAME']['DIF']] + \
            constantDict['MACD'][macd]['PARA']['P2'] * recentExtensionInfo[constantDict['MACD'][macd]['NAME']['DEA']]
          extensionInfo[constantDict['MACD'][macd]['NAME']['MACD']] = \
            (extensionInfo[constantDict['MACD'][macd]['NAME']['DIF']] - extensionInfo[constantDict['MACD'][macd]['NAME']['DEA']]) * 2
        else:
          extensionInfo[constantDict['MACD'][macd]['NAME']['DIF']] = constantDict['ZERO']
          extensionInfo[constantDict['MACD'][macd]['NAME']['DEA']] = constantDict['ZERO']
          extensionInfo[constantDict['MACD'][macd]['NAME']['MACD']] = constantDict['ZERO']
        
        recentExtensionInfo[constantDict['MACD'][macd]['NAME']['DEA']] = extensionInfo[constantDict['MACD'][macd]['NAME']['DEA']]
      #记录数量
      recordNum += 1
      #基本数据及衍生数据写入队列
      #securityInfoQueue.put({'DB':db,'ANA':analysisInfo,'EXT':extensionInfo, 'XRXD':excludeType})
      analysisInfoDict = analysisInfo
      extensionInfoDict = extensionInfo
      #print 'put:',extensionInfo
    securityInfoQueue.put({'DB':db,'ANA':analysisInfoDict,'EXT':extensionInfoDict, 'XRXD':excludeType})
    #print '<< CalculateExtensionInfo'
  except Exception,e:
    print e
  return None
