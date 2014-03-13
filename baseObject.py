# -*- coding: UTF-8 -*-
'''
Created on 2013-11-19

@author: Grayson
'''


class BaseObject(object):
  '''
  Base object class
  '''


  def __init__(self, lens):
    '''
    Constructor, set max list length.
    '''
    self.list = []
    self.lens = lens


  def append(self, close):
    self.list.append(close)
    while (len(self.list) > self.lens):
      self.list.pop(0)
      
      
  def object(self):
    '''
    Return object list.
    '''
    return self.list
  
  
  def max(self):
    '''
    Return max record.
    '''
    return max(self.list)
  
  
  def min(self):
    '''
    Return min record.
    '''
    return min(self.list)
