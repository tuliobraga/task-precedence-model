import sys

class Task(object):

    def __init__(self, id, startTime, endTime, length, executorId):
        super(Task, self).__init__()
        self._id = id
        self._startTime = float(startTime)
        self._endTime = float(endTime)
        self._length = float(length) # in miliseconds
        self._executorId = executorId

    def getLength(self):
        return self._length

    def getEndTime(self):
        return self._endTime

    def getStartTime(self):
        return self._startTime

    def getExecutorId(self):
        return self._executorId

    def __str__(self):
        return '{"task": ' + str(self._id) + ', "taskTime:" ' + str(self._length) + ', "startTime": ' + str(self._startTime) + ', "endTime": ' + str(self._endTime) + ']}'
