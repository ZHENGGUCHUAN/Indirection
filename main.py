# -*- coding: UTF-8 -*-
'''
Created on 2013-11-27

@author: Grayson
'''
from datetime             import datetime, timedelta
from multiprocessing      import Process, Queue
from time                 import time, sleep
from analysisInfo         import AnalysisInfo
from calculateExtension   import CalculateExtensionInfo
#from extensionInfo        import ExtensionInfo
from splitSecurity        import SplitSecurity
from synchroMongo         import SynchroMongo
from constant             import dbDict
from excludeRightDividend import ExcludeRightDividend
from sched                import scheduler
from collections          import OrderedDict
import sys, os, traceback, pymongo
sys.path.append(os.getcwd() + r'\..\Common')
from mssqlAPI             import MssqlAPI
from mongoAPI             import MongoAPI
from dbServer             import mssqlDbServer, mongoDbServer
from logFile              import LogFile
from heartBeat            import HeartBeat


#获取分表名信息
def DisposeSplitSecurity(mssqlDict, mongoDIct):
  splitSecurity = SplitSecurity(mssqlDict, mongoDIct)
  splitSecurityDict = dict()
  for db in dbDict:
    securityCodeList = splitSecurity.GetSecurityCodeList(db)
    splitSecurityDict[db] = splitSecurity.SplitSecurityIndex(securityCodeList, dbDict[db]['SPLIT'])
  return splitSecurityDict


#证券分析信息进程执行
def DisposeAnalysisInfoProc(mssqlDict, db, queue):
  print '>> DisposeAnalysisInfoProc'
  analysisInfo = AnalysisInfo(mssqlDict['SERVER'], mssqlDict['DB'], mssqlDict['USER'], mssqlDict['PWD'])
  analysisInfo.GetAllAnalysisInfo(db, queue)
  print '<< DisposeAnalysisInfoProc'


#计算衍生数据进程
def CalculateExtensionProc(excludeRightDividendDict, analysisQueue, extensionQueue):
  print '>> CalculateExtensionProc'
  for msg in iter(analysisQueue.get, None):
    try:
      CalculateExtensionInfo(msg['DB'], msg['MSG'], excludeRightDividendDict[msg['MSG'][0]['INNER_CODE']], extensionQueue)
    except KeyError:
      CalculateExtensionInfo(msg['DB'], msg['MSG'], list(), extensionQueue)
  print '<< CalculateExtensionProc'


#启动计算衍生数据进程
def DisposeCalculateExtensionProc(processDict, excludeRightDividendDict, analysisQueue, extensionQueue):
  print '>> DisposeCalculateExtensionProc'
  processList = list()
  for num in range(0, processDict['CALCULATE']):
    process = Process(target=CalculateExtensionProc, args=(excludeRightDividendDict, analysisQueue, extensionQueue))
    process.start()
    processList.append(process)
  print '<< DisposeCalculateExtensionProc'
  return processList


'''
#写入MSSQL衍生数据进程
def InsertExtensionIntoMssqlProc(mssqlDict, extensionQueue, mongoQueue):
  #extensionInfo = ExtensionInfo(mssqlDict['SERVER'], mssqlDict['DB'], mssqlDict['USER'], mssqlDict['PWD'])
  for msg in iter(extensionQueue.get, None):
    #同步MongoDB写入
    mongoQueue.put(msg)
    #MSSQL写入操作
    #extensionInfo.SetExtensionInfo(msg)


#启动写入MSSQL衍生数据进程
def DisposeExtensionInfoProc(processDict, mssqlDict, extensionQueue, mongoQueue):
  processList = list()
  for num in range(0, processDict['MSSQL']):
    process = Process(target=InsertExtensionIntoMssqlProc, args=(mssqlDict, extensionQueue, mongoQueue))
    process.start()
    processList.append(process)
  return processList
'''


#写入MongoDB数据进程
def InsertInfoIntoMongoDbProc(mongoDict, mongoQueue, splitSecurityDict):
  print 'InsertInfoIntoMongoDbProc'
  syncMongo = SynchroMongo(mongoDict, splitSecurityDict)
  for msg in iter(mongoQueue.get, None):
    #MongoDB写入操作
    syncMongo.SetExtensionInfoIntoSql(msg)


#启动写入MongoDB数据进程
def DisposeInsertInfoIntoMongoDbProc(processDict, mongoDict, mongoQueue, splitSecurityDict):
  processList = list()
  for num in range(0, processDict['MONGO']):
    process = Process(target=InsertInfoIntoMongoDbProc, args=(mongoDict, mongoQueue, splitSecurityDict))
    process.start()
    processList.append(process)
  return processList


#更新所有证券2013年以后历史数据
def UpdateExtensionInfo(mssqlDict):
  mssqlHandle = MssqlAPI(server = mssqlDict['SERVER'], db = mssqlDict['DB'], user = mssqlDict['USER'], pwd = mssqlDict['PWD'])
  logFile = LogFile(name = 'Indirection')
  try:
    mssqlHandle.sqlExecuteProc('usp_UpdateFrom251', ())
    mssqlHandle.sqlCommit()
    logFile.logInfo('Execute SQL Proc "usp_UpdateFrom251" succeed.')
  except:
    logFile.logInfo('Execute SQL Proc "usp_UpdateFrom251" failed.')
    print traceback.format_exc()


#导入所有证券历史
def CorrectAllSecurityInfo(mssqlDict, mongoDict, processDict, queueDict):
  startTime = datetime.now()
  print 'Start at:',startTime
  print 'DisposeSplitSecurity at',datetime.now()
  #分表名获取
  splitSecurityDict = DisposeSplitSecurity(mssqlDict, mongoDict)
  print 'DisposeAnalysisInfoProc at',datetime.now()
  #证券分析数据查询
  analysisQueue = Queue(queueDict['ANA'])
  monitorProcessDict = dict()
  analysisInfoProcessList = list()
  for db in dbDict:
    process = Process(target=DisposeAnalysisInfoProc, args=(mssqlDict, db, analysisQueue))
    process.start()
    analysisInfoProcessList.append(process)
  monitorProcessDict['Analysis'] = analysisInfoProcessList
  #print 'DisposeCalculateExtensionProc at',datetime.now()
  #证券衍生数据计算
  #extensionQueue = Queue(queueDict['EXT'])
  mongoQueue = Queue(queueDict['MONGO'])
  excludeRightDividend = ExcludeRightDividend(mssqlDict['SERVER'], mssqlDict['DB'], mssqlDict['USER'], mssqlDict['PWD'])
  excludeRightDividendDict = excludeRightDividend.GetExcludeRightDividendInfo()
  calculateExtensionProcList = DisposeCalculateExtensionProc(processDict, excludeRightDividendDict, analysisQueue, mongoQueue)
  monitorProcessDict['Calculate'] = calculateExtensionProcList
  #print 'DisposeExtensionInfoProc at',datetime.now()
  #启动写入MSSQL衍生数据进程
  #extentionInfoProcList = DisposeExtensionInfoProc(processDict, mssqlDict, extensionQueue, mongoQueue)
  #monitorProcessDict['Extension'] = extensionInfoProcList
  #print 'DisposeInsertInfoIntoMongoDbProc at',datetime.now()
  #启动写入MongoDB数据进程
  insertInfoIntoMongoDbProcList = DisposeInsertInfoIntoMongoDbProc(processDict, mongoDict, mongoQueue, splitSecurityDict)
  monitorProcessDict['Mongo'] = insertInfoIntoMongoDbProcList
  terminateNum = 0
  while True:
    #移除已终止的进程
    for processName in monitorProcessDict:
      for process in monitorProcessDict[processName]:
        if (process.is_alive() == False):
          monitorProcessDict[processName].remove(process)
    #检查计算，SQL写入，MongoDB写入进程，如进程意外终止，则重新启动进程
    while (len(monitorProcessDict['Calculate']) < processDict['CALCULATE']):
      process = Process(target=CalculateExtensionProc, args=(excludeRightDividendDict, analysisQueue, mongoQueue))
      process.start()
      monitorProcessDict['Calculate'].append(process)
    while (len(monitorProcessDict['Mongo']) < processDict['MONGO']):
      process = Process(target=InsertInfoIntoMongoDbProc, args=(mongoDict, mongoQueue, splitSecurityDict))
      process.start()
      monitorProcessDict['Mongo'].append(process)
    print datetime.now()
    for processName in monitorProcessDict:
      print processName,len(monitorProcessDict[processName])
    print 'analysisQueue:',analysisQueue.qsize()
    #print 'extensionQueue:',extensionQueue.qsize()
    print 'mongoQueue:',mongoQueue.qsize()
    if ((len(monitorProcessDict['Analysis']) == 0) and 
        (analysisQueue.qsize() == 0) and 
        (mongoQueue.qsize() == 0)):
      terminateNum += 1
    if (terminateNum >= 10):
      #关闭所有进程
      for processName in monitorProcessDict:
        for process in monitorProcessDict[processName]:
          if (process.is_alive() == True):
            process.terminate()
      break
    sleep(10)
    
  finalTime = datetime.now()
  deltaTime = finalTime - startTime
  totalTime = deltaTime.total_seconds()
  totalHour = totalTime // 3600
  totalMin = (totalTime % 3600) // 60
  totalSec = totalTime % 60
  print("Total time: %d(h)%d(m)%d(s)" % (totalHour, totalMin, totalSec))
  logFile = LogFile(name = 'Indirection')
  logFile.logInfo('Correct all security info succeed, total time: ' + str(totalHour) + '(h)' + str(totalMin) + '(m)' + str(totalSec) + '(s)')


def UpdateSplitSecurityIndex(mssqlDict, mongoDict): 
  splitSecurityIndex = SplitSecurity(mssqlDict, mongoDict)
  splitDict = {'DAY':200, 'WEEK':100, 'MONTH':50}
  for item in splitDict:
    #print item
    securityCodeList = splitSecurityIndex.GetSecurityCodeList(item)
    splitRangeList = splitSecurityIndex.SplitSecuritySubarea(securityCodeList, splitDict[item])
    num = 0
    for mongoHandle in splitSecurityIndex.mongoHandle:
      mongoHandle.drop('SplitIndex', item)
    document = OrderedDict()
    for splitRange in splitRangeList:
      document['_id'] = num
      document['Range'] = [splitRange['min'], splitRange['max']]
      #print num,'min:',range['min'],', max:',range['max']
      for mongoHandle in splitSecurityIndex.mongoHandle:
        mongoHandle.insert('SplitIndex', item, document.copy())
      document.clear()
      num += 1


def dropOriginalData(mongoDict):
  logFile = LogFile(name = 'Indirection')
  for mongo in mongoDict:
    try:
      mongoHandle = MongoAPI(server = mongo['SERVER'], port = mongo['PORT'])
      #drop table
      splitTableList = ['KDayData', 'KWeeklyData', 'KMonthData']
      mergeTableList = ['CandleDay', 'CandleWeek', 'CandleMonth']
      for splitTable in splitTableList:
        mongoHandle.drop(splitTable)
      for mergeTable in mergeTableList:
        mongoHandle.drop(mergeTable)
      #create index
      for mergeTable in mergeTableList:
        mongoHandle.createIndex(mergeTable, 'data', [('_id.IC', pymongo.ASCENDING)])
        mongoHandle.createIndex(mergeTable, 'data', [('TO', pymongo.DESCENDING)])
      logFile.logInfo('Drop original data from server: ' + mongo['SERVER'] + ' succeed.')
      del mongoHandle
    except:
      logFile.logInfo('Drop original data from server: ' + mongo['SERVER'] + ' failed.')
      print traceback.format_exc()


def correctProc(mssqlDict, mongoDict, processDict, queueDict):
  #执行存储过程将源数据导入到SQL中间库
  try:
    UpdateExtensionInfo(mssqlDict)
  except:
    print traceback.format_exc()
    timetuple = datetime.now().timetuple()
    if timetuple.tm_hour <= 6:
      updateScheduler = scheduler(time, sleep)
      #schedule run
      updateScheduler.enter(timedelta(minutes=30).total_seconds(), 1, UpdateExtensionInfo, (mssqlDict, mongoDict, processDict, queueDict))
      updateScheduler.run()
    else:
      sys.exit()
  #删除Mongo库过期数据
  try:
    dropOriginalData(mongoDict)
  except:
    print traceback.format_exc()
    timetuple = datetime.now().timetuple()
    if timetuple.tm_hour <= 6:
      updateScheduler = scheduler(time, sleep)
      #schedule run
      updateScheduler.enter(timedelta(minutes=30).total_seconds(), 1, dropOriginalData, (mongoDict))
      updateScheduler.run()
    else:
      sys.exit()
  #计算更新Mongo库历史行情
  try:
    CorrectAllSecurityInfo(mssqlDict, mongoDict, processDict, queueDict)
  except:
    print traceback.format_exc()
    timetuple = datetime.now().timetuple()
    if timetuple.tm_hour <= 6:
      updateScheduler = scheduler(time, sleep)
      #schedule run
      updateScheduler.enter(timedelta(minutes=30).total_seconds(), 1, CorrectAllSecurityInfo, (mssqlDict, mongoDict, processDict, queueDict))
      updateScheduler.run()
    else:
      sys.exit()

  #更新Mongo库分表信息
  try:
    UpdateSplitSecurityIndex(mssqlDict, mongoDict)
  except:
    print traceback.format_exc()
    timetuple = datetime.now().timetuple()
    if timetuple.tm_hour <= 6:
      updateScheduler = scheduler(time, sleep)
      #schedule run
      updateScheduler.enter(timedelta(minutes=30).total_seconds(), 1, UpdateSplitSecurityIndex, (mssqlDict, mongoDict))
      updateScheduler.run()
    else:
      sys.exit()


if __name__ == '__main__':
  mssqlDict = mssqlDbServer['46']['LAN']
  mongoDict = (mongoDbServer['23']['LAN'],)#mongoDbServer['22']['LAN']
  processDict = {'CALCULATE':8,'MSSQL':4,'MONGO':8}
  queueDict = {'ANA':10, 'EXT':10000, 'MONGO':10000}
  correctProc (mssqlDict, mongoDict, processDict, queueDict)
  heartBeat = HeartBeat(name = 'Indirection', interval = 60 * 60)
  heartBeat.startThread()
  while True:
    #Get date time now
    timetuple = datetime.now().timetuple()
    #Set execute time 02:00:00 every morning.
    executeTime = datetime(timetuple.tm_year, timetuple.tm_mon, timetuple.tm_mday, 1)
    #If date time now great than 02:00:00, set next execute time at 02:00:00 tomorrow morning.
    if timetuple.tm_hour >= 1:
      executeTime += timedelta(days=1)
      print("Next execute time: %s" % executeTime.strftime("%Y-%m-%d %X"))
    #Get delta time within next execute time and date time now.
    deltaTime = executeTime - datetime.now()
    print("Delta time: %f (seconds)" % deltaTime.total_seconds())

    updateScheduler = scheduler(time, sleep)
    #schedule run
    updateScheduler.enter(deltaTime.total_seconds(), 1, correctProc, (mssqlDict, mongoDict, processDict, queueDict))
    updateScheduler.run()
