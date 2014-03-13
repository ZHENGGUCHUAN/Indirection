# -*- coding: UTF-8 -*-
'''
Created on 2013-10-09

@author: Grayson
'''
import datetime

def convertStockId(stockId, market="", securityType=2):
  #market: "SH" or "SZ"
  #type: 0 - stock, 1 - index, 2 - all
  try:
    if (len(stockId) != 6):
      raise UserWarning, "stockId: %s invalid." % stockId
    
    inwardId = stockId
    if (securityType == 0):         #stock
      if (stockId[0:2] == "60"):    #--SH
        inwardId = "11" + inwardId
      else:                         #--SZ
        inwardId = "12" + inwardId
    elif (securityType == 1):       #index
      if (stockId[0:2] == "00"):    #--SH
        inwardId = "21" + inwardId
      else:                         #--SZ
        inwardId = "22" + inwardId
    elif (securityType == 2):       #all
      if (market == "SH"):          #--SH
        inwardId = "1" + inwardId
        if (stockId[0:2] == "00"):  #----index
          inwardId = "2" + inwardId
        else:                       #----stock
          inwardId = "1" + inwardId
      elif (market == "SZ"):        #--SZ
        inwardId = "2" + inwardId
        if (stockId[0:3] == "399"): #----index
          inwardId = "2" + inwardId
        else:                       #----stock
          inwardId = "1" + inwardId
      else:
        raise UserWarning, "market: %s invalid." % market
    else:
      raise UserWarning, "type: %d invalid." % type
  except:
    print("Convert stock: %s, market: %s failed, type: %d." % stockId, market, type)
  
  return inwardId


def datetime2seconds(dateTime):
  '''
  Convert a datetime format time to second
    from:2006-04-12 16:46:40 to:23123123
  '''
  if (type(dateTime) == type(None)):
    return 0
  else:
    return (dateTime - datetime.datetime(1970,1,1)).total_seconds()
  
  
def datetime2days(dateTime):
  '''
  Convert a datetime format time to day
    from:2013-09-5 to:15959
  '''
  return int(datetime2seconds(dateTime) // (24 * 60 * 60))


def equalZero(value):
  '''
  Compare a float value equal zero.
  '''
  if ((value > -1e-8) and (value < 1e-8)):
    return True
  else:
    return False
  
  
def rightType(bonus, changeRatio):
  '''
  Return exclude Right Status:
  0 - 除权
  1 - 除息
  2 - 除权除息
  3 - 普通状态
  '''
  if (equalZero(bonus) and (not equalZero(changeRatio))):
    return 0
  elif ((not equalZero(bonus)) and equalZero(changeRatio)):
    return 1
  elif ((not equalZero(bonus)) and (not equalZero(changeRatio))):
    return 2
  else:
    return 3
