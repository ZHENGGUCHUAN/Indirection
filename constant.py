# -*- coding: UTF-8 -*-
'''
Created on 2013-11-27

@author: Grayson
'''
from decimal import Decimal

#Analysis table name，日周月SQL基础信息数据库表名
dbAnalysisDayTable = '[QN_Quotation].[dbo].[SECURITY_ANALYSIS_DAY]'
dbAnalysisWeekTable = '[QN_Quotation].[dbo].[SECURITY_ANALYSIS_WEEK]'
dbAnalysisMonthTable = '[QN_Quotation].[dbo].[SECURITY_ANALYSIS_MONTH]'
#Extension table name，日周月SQL衍生信息数据库表名
dbExtensionDayTable = '[QN_Quotation].[dbo].[SECURITY_EXTENSION_DAY]'
dbExtensionWeekTable = '[QN_Quotation].[dbo].[SECURITY_EXTENSION_WEEK]'
dbExtensionMonthTable = '[QN_Quotation].[dbo].[SECURITY_EXTENSION_MONTH]'
#Offset table name，日周月SQL时间偏移数据库表名
dbOffsetDayTable = '[QN_Quotation].[dbo].[SECURITY_OFFSET_DAY]'
dbOffsetWeekTable = '[QN_Quotation].[dbo].[SECURITY_OFFSET_WEEK]'
dbOffsetMonthTable = '[QN_Quotation].[dbo].[SECURITY_OFFSET_MONTH]'
#MongoDB name
dbMongoDay = 'KDayData'
dbMongoWeek = 'KWeeklyData'
dbMongoMonth = 'KMonthData'
#Statistics table name，分析数据、衍生数据各股最新更新时间
dbAnalysisTable = '[QN_Quotation].[dbo].[SECURITY_ANALYSIS_STATISTICS]'
dbExtensionTable = '[QN_Quotation].[dbo].[SECURITY_EXTENSION_STATISTICS]'
#Trade date，日周月SQL信息交易时间字段名
colTradeDateDay = '[SECURITY_ANALYSIS].[TRADE_DATE] AS TRADE_DATE'
colTradeDateWeek = '[SECURITY_ANALYSIS].[TRADE_YEAR] AS TRADE_YEAR, [SECURITY_ANALYSIS].[TRADE_WEEK] AS TRADE_WEEK, [SECURITY_ANALYSIS].[LAST_TRADE_DATE] AS LAST_TRADE_DATE'
colTradeDateMonth = '[SECURITY_ANALYSIS].[TRADE_YEAR] AS TRADE_YEAR, [SECURITY_ANALYSIS].[TRADE_MONTH] AS TRADE_MONTH, [SECURITY_ANALYSIS].[LAST_TRADE_DATE] AS LAST_TRADE_DATE'
#Base info column name，日周月SQL基础信息数据库字段名称
colAnalysis = ['INNER_CODE', 'LCLOSE', 'TOPEN', 'TCLOSE', 'THIGH', 'TLOW', 'TVOLUME', 'TVALUE', 'CHNG', 'EXCHR']
colAnalysisDay = colAnalysis + ['TRADE_DATE'] 
colAnalysisWeek = colAnalysis + ['TRADE_YEAR', 'TRADE_WEEK', 'LAST_TRADE_DATE']
colAnalysisMonth = colAnalysis + ['TRADE_YEAR', 'TRADE_MONTH', 'LAST_TRADE_DATE']
#Extend info column name，日周月SQL衍生信息数据库字段名称
colExtension = ['INNER_CODE', 'MA5', 'MA10', 'MA13', 'MA14', 'MA20', 'MA25', 'MA43', 'VOL5', 'VOL10', 'VOL30', 'VOL60', 'VOL135', \
                'RSV9', 'K9', 'D3', 'J3', 'RSV34', 'K34', 'D9', 'J9', 'EMA5', 'EMA6', 'EMA12', 'EMA13', 'EMA26', 'EMA35', \
                'DIF1', 'DEA1', 'MACD1', 'DIF2', 'DEA2', 'MACD2', 'DIF3', 'DEA3', 'MACD3']
colExtensionDay = colExtension + ['TRADE_DATE']
colExtensionWeek = colExtension + ['TRADE_YEAR', 'TRADE_WEEK']
colExtensionMonth = colExtension + ['TRADE_YEAR', 'TRADE_MONTH']
#Query order，日周月SQL信息查询排序依据
orderByDay = 'TRADE_DATE'
orderByWeek = 'TRADE_YEAR, TRADE_WEEK'
orderByMonth = 'TRADE_YEAR, TRADE_MONTH'
#Database info dictionary，日周月SQL信息查询用字典，包括：数据表名称，交易时间字段名称，基础信息字段列表，衍生信息字段列表，查询排序依据

dbDict = {'DAY'  :{'TABLE'  :{'ANA':dbAnalysisDayTable,   'EXT':dbExtensionDayTable,  'TO':dbOffsetDayTable,  'MONGO':dbMongoDay,   'MERGE':'CandleDay'}, \
                   'COLUMN' :{'ANA':colAnalysisDay,       'EXT':colExtensionDay,      'TO':'DAYS'}, \
                   'TRADEDATE':colTradeDateDay,  'ORDER':orderByDay,  'SPLIT':200}, \
                   
          'WEEK' :{'TABLE'  :{'ANA':dbAnalysisWeekTable,  'EXT':dbExtensionWeekTable, 'TO':dbOffsetWeekTable, 'MONGO':dbMongoWeek,  'MERGE':'CandleWeek'}, \
                   'COLUMN' :{'ANA':colAnalysisWeek,      'EXT':colExtensionWeek,     'TO':'WEEKS'}, \
                   'TRADEDATE':colTradeDateWeek, 'ORDER':orderByWeek, 'SPLIT':100}, \
          'MONTH':{'TABLE'  :{'ANA':dbAnalysisMonthTable, 'EXT':dbExtensionMonthTable,'TO':dbOffsetMonthTable,'MONGO':dbMongoMonth, 'MERGE':'CandleMonth'}, \
                   'COLUMN' :{'ANA':colAnalysisMonth,     'EXT':colExtensionMonth,    'TO':'MONTHS'}, \
                   'TRADEDATE':colTradeDateMonth,'ORDER':orderByMonth,'SPLIT':50}}


kdjTuple = ((9,3,3),(34,9,9))
emaTuple = (5,6,12,13,26,35)
macdTuple = ((12,26,9),(6,13,5),(5,35,5))

def getKDJConstantDict(kdjTuple):
  '''
  Create and return KDJ constant dictionary.
  
  KDJ输入参数，格式如下：
  ((K参数1,D参数1,J参数1),(K参数2,D参数2,J参数2),... ...)
  
  KDJ输出常数字典，格式如下：
  {(<K参数1,D参数1，J参数1>):{'NAME':{'RSV':<RSV名称1>,'K':<K名称1>,'D':<D名称1>,'J':<J名称1>},
                         'PARA':{'KP1':<K值计算第一参数1>,'KP2':<K值计算第二参数1>,'DP1':<D值计算第一参数1>,'DP2':<D值计算第二参数1>}},
   (<K参数2,D参数2，J参数2>):{'NAME':{'RSV':<RSV名称2>,'K':<K名称2>,'D':<D名称2>,'J':<J名称2>},
                         'PARA':{'KP1':<K值计算第一参数2>,'KP2':<K值计算第二参数2>,'DP1':<D值计算第一参数2>,'DP2':<D值计算第二参数2>}},
   ... ...}
  '''
  try:
    kdjConstantDict = {}
    for kdj in kdjTuple:
      kdjConstantDict[kdj] = {'NAME':{'RSV':'RSV'+str(kdj[0]),'K':'K'+str(kdj[0]),'D':'D'+str(kdj[1]),'J':'J'+str(kdj[2])},
                              'PARA':{'KP1':1-(Decimal(1)/kdj[1]),'KP2':Decimal(1)/kdj[1],'DP1':1-(Decimal(1)/kdj[2]),'DP2':Decimal(1)/kdj[2]}}
    return kdjConstantDict
  except IndexError:
    print 'Function getKDJConstantDict() index error.'
    
    
def getEMAConstantDict(emaTuple):
  '''
  Create and return EMA constant dictionary.
  
  EMA输入参数，格式如下：
  (EMA参数1,EMA参数2,EMA参数3,... ...)
  
  EMA输出常数字典，格式如下：
  {<EMA名称1>:{'NAME':<EMA名称1>,'PARA':{'P1':<第一参数1>,'P2':<第二参数1>}},
   <EMA名称2>:{'NAME':<EMA名称2>,'PARA':{'P1':<第一参数2>,'P2':<第二参数2>}},
   ... ...}
  '''
  try:
    emaConstantDict = {}
    for ema in emaTuple:
      emaConstantDict['EMA'+str(ema)] = {'NAME':'EMA'+str(ema),
                                         'PARA':{'P1':Decimal(2)/(ema+1),'P2':1-(Decimal(2)/(ema+1))}}
    return emaConstantDict
  except IndexError:
    print 'Function getEMAConstantDict() index error.'
    
    
def getMACDConstantDict(macdTuple):
  '''
  Create and return MACD constant dictionary.
  
  MACD输入参数，格式如下：
  ((<DIF参数1,<DEA参数1>,<MACD参数1>),(<DIF参数2,<DEA参数2>,<MACD参数2>),... ...)
  
  MACD输出常数字典，格式如下：
  {(<DIF参数1>,<DEA参数1>,<MACD参数1>):{'NAME':{'DIF':'DIF1','DEA':'DEA1','MACD':'MACD1'},'PARA':{'P1':<第一参数1>,'P2':<第二参数1>}},
   (<DIF参数2>,<DEA参数2>,<MACD参数2>):{'NAME':{'DIF':'DIF2','DEA':'DEA2','MACD':'MACD2'},'PARA':{'P1':<第一参数2>,'P2':<第二参数2>}},
   ... ...}
  '''
  try:
    macdConstantDict = {}
    num = 1
    for macd in macdTuple:
      macdConstantDict[macd] = {'NAME':{'DIF':'DIF'+str(num),'DEA':'DEA'+str(num),'MACD':'MACD'+str(num)},
                                'PARA':{'P1':Decimal(2)/(macd[2]+1),'P2':1-(Decimal(2)/(macd[2]+1))}}
      num += 1
    return macdConstantDict
  except IndexError:
    print 'Function getMACDConstantDict index error.'
    
    
constantDict = {'KDJ'    :getKDJConstantDict(kdjTuple),
                'EMA'    :getEMAConstantDict(emaTuple), 
                'MACD'   :getMACDConstantDict(macdTuple),
                'ZERO'   :Decimal(0)}

