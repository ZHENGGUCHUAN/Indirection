# -*- coding: UTF-8 -*-
'''
Created on 2013-12-06

@author: Grayson
'''
from collections   import OrderedDict
from constant      import dbDict
from common        import datetime2days
import sys, os, traceback
sys.path.append(os.getcwd() + r'\..\Common')
from mongoAPI      import MongoAPI


class SynchroMongo(object):
  '''
  classdocs
  '''


  def __init__(self, mongoDict, colNameDict):
    '''
    Constructor
    '''
    self.mongoList = list()
    for mongo in mongoDict:
      mongo = MongoAPI(mongo['SERVER'], mongo['PORT'])
      self.mongoList.append(mongo)
    self.colNameDict = colNameDict
    
    
    
  def __del__(self):
    del self.mongo
    self.colNameDict = {}
    del self.colNameDict
    
    
  def SetExtensionInfoIntoSql(self, msg):
    try:
      infoDict = OrderedDict()
      idDict = OrderedDict()
      if (msg['DB'] == 'DAY'):
        timeOffset = datetime2days(msg['ANA']['TRADE_DATE'])
        idDict['IC'] = int(msg['ANA']['INNER_CODE'])
        idDict['TO'] = timeOffset
        infoDict['_id'] = idDict
        infoDict['TO'] = timeOffset
      elif (msg['DB'] == 'WEEK'):
        idDict['IC'] = int(msg['ANA']['INNER_CODE'])
        idDict['YR'] = msg['ANA']['TRADE_YEAR']
        idDict['WK'] = msg['ANA']['TRADE_WEEK']
        infoDict['_id'] = idDict
        infoDict['TO'] = datetime2days(msg['ANA']['LAST_TRADE_DATE'])
      elif (msg['DB'] == 'MONTH'):
        idDict['IC'] = int(msg['ANA']['INNER_CODE'])
        idDict['YR'] = msg['ANA']['TRADE_YEAR']
        idDict['MN'] = msg['ANA']['TRADE_MONTH']
        infoDict['_id'] = idDict
        infoDict['TO'] = datetime2days(msg['ANA']['LAST_TRADE_DATE'])
      infoDict['OP'] = float(msg['ANA']['TOPEN'])
      infoDict['CP'] = float(msg['ANA']['TCLOSE'])
      infoDict['HP'] = float(msg['ANA']['THIGH'])
      infoDict['LP'] = float(msg['ANA']['TLOW'])
      infoDict['VL'] = long(msg['ANA']['TVOLUME'])
      infoDict['AM'] = float(msg['ANA']['TVALUE'])
      if ((msg['ANA']['EXCHR'] == None) or (msg['ANA']['EXCHR'] == 0)):
        infoDict['TR'] = 0
      else:
        infoDict['TR'] = float(msg['ANA']['EXCHR'])
      infoDict['CA'] = float(msg['ANA']['CHNG'])
      infoDict['AA'] = [float(msg['EXT']['MA5']), float(msg['EXT']['MA10']), float(msg['EXT']['MA13']), 
                        float(msg['EXT']['MA14']), float(msg['EXT']['MA20']),float(msg['EXT']['MA25']), 
                        float(msg['EXT']['MA43'])]
      infoDict['AV'] = [long(msg['EXT']['VOL5']), long(msg['EXT']['VOL10']), long(msg['EXT']['VOL30']), 
                        long(msg['EXT']['VOL60']), long(msg['EXT']['VOL135'])]
      infoDict['KV'] = [float(msg['EXT']['K9']), float(msg['EXT']['K34'])] 
      infoDict['DV'] = [float(msg['EXT']['D3']), float(msg['EXT']['D9'])]
      infoDict['JV'] = [float(msg['EXT']['J3']), float(msg['EXT']['J9'])]
      infoDict['DIF'] = [float(msg['EXT']['DIF1']), float(msg['EXT']['DIF2']), float(msg['EXT']['DIF3'])]
      infoDict['DEA'] = [float(msg['EXT']['DEA1']), float(msg['EXT']['DEA2']), float(msg['EXT']['DEA3'])]
      infoDict['MACD'] = [float(msg['EXT']['MACD1']), float(msg['EXT']['MACD2']), float(msg['EXT']['MACD3'])]
      infoDict['ER'] = int(msg['XRXD'])
      infoDict['RSV'] = [float(msg['EXT']['RSV9']), float(msg['EXT']['RSV34'])]
      infoDict['EMA'] = [float(msg['EXT']['EMA5']), float(msg['EXT']['EMA6']), float(msg['EXT']['EMA12']),  
                         float(msg['EXT']['EMA13']), float(msg['EXT']['EMA26']), float(msg['EXT']['EMA35'])]
      
      for mongo in self.mongoList:
        mongo.update(dbDict[msg['DB']]['TABLE']['MONGO'], 'data_'+self.colNameDict[msg['DB']][msg['EXT']['INNER_CODE']], {'_id':idDict}, infoDict)
        #并表数据写入
        if (msg['WMT']):
          mongo.update(dbDict[msg['DB']]['TABLE']['MERGE'], 'data', {'_id':idDict}, infoDict)
      #print dbDict[msg['DB']]['TABLE']['MONGO'],'data_'+self.colNameDict[msg['DB']][msg['EXT']['INNER_CODE']]
      #print idDict
      #print infoDict
    except Exception,e:
      print 'Set extension info into MSSQL failed,',e
      print traceback.format_exc()
