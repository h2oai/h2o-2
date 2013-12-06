##
# A set of classes that do stuff:
# Classes are task managers for respect task type. 
# At the head of control is the WriterTask, which
# control to the other writers.
##

from mungeTasks import *

def switch(x):
    return {
        'mungeTask': MungeWriter(),
        #'classTask': ClassWriter(),
        #'regreTask': RegressWriter(),
        #'clustTask': ClusterWriter(),
        #'modelTask': ModelWriter(),
        'filterTask': FilterWriter(),
        #'multiVecTask': MultiVecTaskWriter(),
        #'vecTask': VecTaskWriter(),
        #'frameTask': FrameTaskWriter(),
    }[x]

#Main Writer class that diverts work to MungeWriter, ClassWriter, RegressWriter, ClusterWriter, ModelWriter
class WriterTask:
    def __init__(self):
        self._data     = None
        self._dataPath = None
        self._taskType = None
        self._dataTask = None
        self._FUParams = None
        self._FU       = None
    
    def setData(self, data, dataPath):
        self._data = data
        self._dataPath = dataPath

    def setType(self, task):
        self._taskType = task

    def setDataTask(self, dataTask):
        self._dataTask = dataTask
    
    def setFUParams(self, FUParams):
        self._FUParams = FUParams
    
    def setFU(self, FU):
        self._FU = FU

    def invoke(self, object):
        object._data = self._data
        object._dataPath = self._dataPath
        #object._dataTask = self._dataTask
        object._FUParams = self._FUParams
        object._FU = self._FU
        print "Invoking task: {0}".format(self._dataTask)
        return object.invoke(switch(self._dataTask))
        
#MungeWriter switches out to Filter, MultiVec, Vec, or FrameTask writers
class MungeWriter:
    def __init__(self): 
        self._data = None
        self._dataPath = None
        self._FUParams = None
        self._FU = None
    
    def invoke(self, object):
        object._data = self._data
        object._dataPath = self._dataPath
        object._FUParams = self._FUParams
        object._FU = self._FU
        return object.writeIt()

#Writes an RUnit filter task, exercises '[' and filter ops
class FilterWriter:
    def __init__(self):
        self._data = None
        self._dataPath = None
        self._FUParams = None
        self._FU = None

    def writeIt(self):
        if self._FU == "[":
            return writeSimpleSliceTestTask(self._FU, self._data, self._dataPath, self._FUParams)
    
        if self._FU in ['<', '>', '<=', '>=', '!=', '==']:
           return writeSimpleNumericFilterTestTask(self._FU, self._data, self._dataPath, self._FUParams)
            
        return writeCompoundFilterTestTask(self._FU, self._data, self._dataPath, self._FUParams)
    
#writes an Runit multivec task, exercises binop2 
#class MultiVecTaskWriter:
#    def __init__(self):
#        self._data = None
#        self._dataPath = None
#        self._FUParams = None
#
#    def writeIt():
#        #unimpl
#
##writes a single vec task, (e.g. slice, sign, ...), exercises unop 
#class VecTaskWriter:
#    def __init__(self):
#        self._data = None
#        self._dataPath = None
#        self._FUParams = None
#
#    def writeIt():
#        #unimpl
#
##writes a frame task, exercises things like summary, head, tail
#class FrameTaskWriter:
#    def __init__(self):
#        self._data = None
#        self._dataPath = None
#        self._FUParams = None
#
#    def writeIt():
#        #unimpl
#
##does show on a model
#class ModelWriter:
#    def __init__(self):
#        self._data = None
#        self._dataPath = None
#        self._FUParams = None
#
#    def writeIt():
#        #unimpl
#
##KMeans
#class ClusterWriter:
#    def __init__(self):
#        self._data = None
#        self._dataPath = None
#        self._FUParams = None
#
#    def writeIt():
#        #unimpl
#
##GLM, DRF Regression, GBM Regression, NN Reg
#class RegressWriter:
#    def __init__(self):
#        self._data = None
#        self._dataPath = None
#        self._FUParams = None
#
#    def writeIt():
#        #unimpl
#
##RF, GBM, NN, LR
#class ClassWriter:
#    def __init__(self):
#        self._data = None
#        self._dataPath = None
#        self._FUParams = None
#
#    def writeIt():
#        #unimpl
#
