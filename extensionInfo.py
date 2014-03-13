# -*- coding: UTF-8 -*-
'''
Created on 2013-10-09

@author: Grayson
'''
from constant import dbDict
import sys, os
sys.path.append(os.getcwd() + r'\..\Common')
from mssqlAPI import MssqlAPI



class ExtensionInfo(object):
  '''
  Security extension info
  '''


  def __init__(self, server, db, user, pwd):
    '''
    Constructor
    '''
    self.mssqlHandle = MssqlAPI(server, db, user, pwd)
        
    
  def __del__(self):
    self.mssqlHandle.sqlCommit()
    del self.mssqlHandle
    
  
  def SetExtensionInfo(self, msg):
    columnStr = ''
    valueStr = ''
    for key in msg['EXT']:
      columnStr += key + ','
      if (key in ('MA5', 'MA10', 'MA13', 'MA14', 'MA20', 'MA25', 'MA43', 'VOL5', 'VOL10', 'VOL30', 'VOL60', 'VOL135')):
        valueStr += 'convert(numeric(18,4),' + str(msg['EXT'][key]) + '),'
      elif (key in ('RSV9', 'K9', 'D3', 'J3', 'RSV34', 'K34', 'D9', 'J9', 'EMA5', 'EMA6', 'EMA12', 'EMA13', 'EMA26', 'EMA35', \
                    'DIF1', 'DEA1', 'MACD1', 'DIF2', 'DEA2', 'MACD2', 'DIF3', 'DEA3', 'MACD3')):
        valueStr += 'convert(numeric(9,4),' + str(msg['EXT'][key]) + '),'
      else:
        valueStr += "'" + str(msg['EXT'][key]) + "',"
    columnStr = columnStr[0:-1]
    valueStr = valueStr[0:-1]
    sqlCmd = '''
      INSERT INTO
      ''' + dbDict[msg['DB']]['TABLE']['EXT'] + '''
      (''' + columnStr + ''')
      VALUES
      ('''+ valueStr +''');
      '''
    self.mssqlHandle.sqlAppend(sqlCmd)
    