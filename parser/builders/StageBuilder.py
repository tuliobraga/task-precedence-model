from ..models.Stage import Stage

# StageBuilder is a mutable object and should only be used to create a immutable Stage object!"
class StageBuilder(object):

    def __init__(self):
        super(StageBuilder, self).__init__()
        self._id = ""
        self._tasks = {}
        self._submissionTime = 0
        self._completionTime = 0
        self._firstTaskStartTime = 0
        self._lastTaskEndTime = 0

    def start(self, id):
        self._id += ":" + str(id)
        self._isFirstTask = True

    def finish(self, submissionTime, completionTime):
        self._submissionTime += submissionTime
        self._completionTime += completionTime
        self._lastTaskEndTime += self._lastTaskEndTimeAux

    def startTask(self, taskId, startTime):
        if self._isFirstTask:
            self._firstTaskStartTime += startTime
            self._isFirstTask = False
        task = self._tasks[taskId]
        task.start(taskId, startTime)

    def finishTask(self, taskId, endTime):
        task = self._tasks[taskId]
        self._lastTaskEndTimeAux = endTime
        task.finish(endTime)

    def addTask(self, taskId, task):
        self._tasks[taskId] = task

    def build(self, counter):
        self._submissionTime /= counter
        self._completionTime /= counter
        self._firstTaskStartTime /= counter
        self._lastTaskEndTime /= counter
        self._length = self._completionTime - self._submissionTime
        self._executionLength = self._lastTaskEndTime - self._firstTaskStartTime

        # Building Stage immutable objects
        tasks = []
        for tid in self._tasks:
            task = self._tasks[tid]
            tasks.append(task.build(counter))

        return Stage(self._id, self._submissionTime, self._completionTime, self._firstTaskStartTime, self._lastTaskEndTime, self._length, self._executionLength, tasks)

    def __str__(self):
        return str("STAGE_" + self._id + ":" + self._submissionTime)





# def _setSubmissionTime(self, time):
#     self._submissionTime = time
#
# def setCompletionTime(self, time):
#     if not self._submissionTime:
#         raise ValueError("Submission time is undefined!")
#
#     self._completionTime = time
#     self._computeStageLength()
