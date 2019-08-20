from ..models.Task import Task

# TaskBuilder is a mutable object and should only be used to create a immutable Task object!"
class TaskBuilder(object):

    def __init__(self, executorId):
        super(TaskBuilder, self).__init__()
        self._startTime = 0
        self._endTime = 0
        self._executorId = executorId

    def start(self, id, startTime):
        self._id = str(id)
        self._startTime += startTime

    def finish(self, endTime):
        self._endTime += endTime

    def build(self, counter):
        self._startTime /= counter
        self._endTime /= counter
        self._computeTaskLength()
        return Task(self._id, self._startTime, self._endTime, self._length, self._executorId)

    def _computeTaskLength(self):
        self._length = self._endTime - self._startTime

    def __str__(self):
        return str("TASK_" + self._id + ":" + self._startTime)

# def setEndTime(self, time):
#     self._endTime = time
#     self._computeTaskLength()
