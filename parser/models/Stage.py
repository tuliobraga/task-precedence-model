class Stage(object):

    def __init__(self, id, submissionTime, completionTime, firstTaskStartTime, lastTaskEndTime, length, execLength, tasks):
        super(Stage, self).__init__()
        self._id = id
        self._submissionTime = float(submissionTime)
        self._completionTime = float(completionTime)
        self._firstTaskStartTime = float(firstTaskStartTime)
        self._lastTaskEndTime = float(lastTaskEndTime)
        self._length = float(length) # in miliseconds
        self._executionLength = float(execLength) # last task end - first task start
        self._tasks = tasks

    def getId(self):
        return self._id

    def getLength(self):
        return self._length

    def getExecutionLength(self):
        return self._executionLength

    def getTasks(self):
        return self._tasks

    def getSubmissionTime(self):
        return self._submissionTime

    def getCompletionTime(self):
        return self._completionTime

    def getLastTaskEndTime(self):
        return self._lastTaskEndTime

    def getFirstTaskStartTime(self):
        return self._firstTaskStartTime

    def calcConcurrencyInterval(self, j):
        start = max(self._submissionTime, j.getSubmissionTime())
        end = min(self._completionTime, j.getCompletionTime())
        # start = max(self.getFirstTaskStartTime(), j.getFirstTaskStartTime())
        # end = min(self.getLastTaskEndTime(), j.getLastTaskEndTime())
        interval = end - start
        return interval if interval > 0 else 0

    def __str__(self):
        result = '{"stage": ' + str(self._id) + ', "stageTime:" ' + str(self._length) + ', "tasks": ['
        first = True
        for t in self._tasks:
            if not first:
                result += ', '
            result += str(t)
            first = False
        result += ']}'
        return result
