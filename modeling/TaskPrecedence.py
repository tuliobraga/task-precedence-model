from ..util.toposort import toposort
import time, os, math

def write_file_from_matrix(filename, data):
	filedata = ""
	for i in data:
		for j in i:
			filedata += "%f " % j
		filedata += "\n"

	write_file(filename, filedata)

def write_file(filename, data):
	text_file = open(filename, "w")
	text_file.write(data)
	text_file.close()

#
# Assumes that Spark Jobs run in FIFO since it is the default job scheduling policy.
#
class TaskPrecedence(object):

    def __init__(self, app, cluster):
        super(TaskPrecedence, self).__init__()
        self._app = app
        self._cluster = cluster

    # int coresToPredict: extrapolation only - to predict the response time of a
    # application executing on a cluster with a different amount of cores than
    # found in the logs
    def run(self, coresToPredict=False):
        if coresToPredict != False and coresToPredict <= 0:
            print "ERROR: Invalid amount of cores to extrapolate."
            sys.exit()

        # init overlap matrix
        totalStages = self._app.getNumStages()
        overlap = self._initMatrix(totalStages, totalStages)

        # init demand and response matrix
        demand = []
        response = []
        offset = 0
        for job in self._app.getJobs():
            if coresToPredict == False:
                numCores = self._cluster.getTotalCores()
                self.intrapolation(job, demand, response, overlap, offset)
            else:
                numCores = coresToPredict
                self.extrapolation(job, demand, response, overlap, offset, coresToPredict)

            offset += job.getNumStages()

        # Save model input files
        self._saveInputFiles(demand, response, overlap)
        # reduce the model with an approximated MVA
        return self._runModel(totalStages, numCores)

    def intrapolation(self, job, demand, response, overlap, offset):
        map = self._computeStageMap(job)
        self.includeJobOverlap(job, map, overlap, offset)
        self.includeJobDemandAndResponse(job, demand, response)

    def extrapolation(self, job, demand, response, overlap, offset, coresToPredict):
        map = self._computeStageMap(job)
        self.includeJobOverlap(job, map, overlap, offset)
        self.extrapolateJobDemandAndResponse(job, demand, response, coresToPredict)

    # Compute an n x n matrix representing the overlap factor between pair of tasks
    # value are 0 for no overlap, or 1 for overlap
    def includeJobOverlap(self, job, stageMap, overlap, offset):
        # computing overlap factors accordingly to the topological order of the DAG
        precedenceList = list(toposort(job.getDag()))
        for concurrentStages in precedenceList:
            for sid_i in concurrentStages:
                i = stageMap[sid_i]
                stage_i = job.getStage(sid_i)
                for sid_j in concurrentStages:
                    j = stageMap[sid_j]
                    stage_j = job.getStage(sid_j)

                    # compute overlap factor
                    if sid_i == sid_j:
                        # a stage don't compete for resources with itself
                        overlap[i+offset][j+offset] = 0.0
                    else:
                        overlap[i+offset][j+offset] = 1*(stage_i.calcConcurrencyInterval(stage_j)/stage_i.getLength())

    def includeJobDemandAndResponse(self, job, demand, response):
        numCores = self._cluster.getTotalCores()
        for sid in job.getStages():
            stage = job.getStage(sid)

            # stage submission time - stage completion time
            r = stage.getLength()
            response.append(numCores*[r])

            # last task end - first task start
            # d = self._averagedDemand(stage.getTasks(), numCores)
            # d = self._demandScheduling(stage.getTasks(), numCores)
            d = stage.getExecutionLength()
            demand.append(numCores*[d])

    def extrapolateJobDemandAndResponse(self, job, demand, response, coresToPredict):
        logCores = self._cluster.getTotalCores()
        factor = float(logCores)/float(coresToPredict)

        for sid in job.getStages():
            stage = job.getStage(sid)

            # since the average task time includes the tasks in the last round too
            # we opt to consider only the integer div between tasks and cores
            # to compute the number of rounds executing tasks
            # numTasks = len(stage.getTasks())
            # avg = sum([t.getLength() for t in stage.getTasks()])/numTasks
            # diff = stage.getExecutionLength() - avg*(numTasks/logCores)
            diff = 0

            # stage submission time - stage completion time
            r = (stage.getLength()*factor) + diff
            response.append(coresToPredict*[r])

            # last task end - first task start
            # d = self._averagedDemand(stage.getTasks(), numCores)
            # d = self._demandScheduling(stage.getTasks(), numCores)
            d = (stage.getExecutionLength()*factor) + diff
            demand.append(coresToPredict*[d])

    def _averagedDemand(self, tasks, numCores):
        numTasks = float(len(tasks))
        avg = sum([t.getLength() for t in tasks])/numTasks
        extra = 1.0 if (numTasks%numCores) > 0 else 0.0
        rounds = (numTasks/numCores) + extra
        return avg*rounds

    def _demandScheduling(self, tasks, numCores):
    	servers = []
    	for i in xrange(0, numCores):
    		servers.append(0.0)

    	for idx, task in enumerate(tasks):
            servers[idx%numCores] += task.getLength()

    	time_1_server = max(servers)
    	return time_1_server

    def _computeStageMap(self, job):
        # mapping stage id to 0..n indexes
        map = {}
        i = 0
        for sid in job.getStages():
            map[sid] = i
            i += 1

        return map
        # for job in self._app.getJobs():
        #     overlap = job.computeOverlapMatrix()
        #     demand = job.computeDemandMatrix(self._cluster.getTotalCores())
        #     response = demand

    def _initMatrix(self, m, n):
        matrix = []
        for i in range(0, m):
            matrix.append(n*[0.0])

        return matrix

    def _saveInputFiles(self, demand, response, overlap):
    	dir_path = os.path.dirname(os.path.realpath(__file__))
    	write_file_from_matrix(dir_path+"/../../temp/response.txt", response)
    	write_file_from_matrix(dir_path+"/../../temp/demand.txt", demand)
    	write_file_from_matrix(dir_path+"/../../temp/overlap.txt", overlap)

    def _runModel(self, numStages, numCores):
    	# dir_path = os.path.dirname(os.path.realpath(__file__))
    	# os.popen("gcc -o %s/../../bin/makva %s/../../bin/makva.c" % (dir_path, dir_path))
    	dir_path = os.path.dirname(os.path.realpath(__file__))
    	cmd = "%s/../../bin/makva -N %s -C %s -e 50 -r %s/../../temp/response.txt -s %s/../../temp/demand.txt -o %s/../../temp/overlap.txt" % (dir_path, numStages, numCores, dir_path, dir_path, dir_path)
    	start_time = time.time()
    	output = os.popen(cmd).read()
    	elapsed_time = time.time() - start_time
    	return float(output.replace("\n","").replace("R: ", "").strip())/numCores, elapsed_time*1000

    # def _reduce(self, response, demand, overlap, numStages, numCores, tolerance):
    #     Q = self._initMatrix(numStages, numCores)
    #     A = self._initMatrix(numStages, numCores)
    #
    #     # Get initial response time values
    #     R_l = 0.0;
    #     R_s = 0.0;
    #     for i in range(0, numStages):
    #         for k in range(0, numCores):
    #             R_s += response[i][k];
    #
    #     converged = 0;
    #     while converged == 0:
    #
    #         # Calculate reduced population residence times
    #         for j in range(0, numStages):
    #             for i in range(0, numStages):
    #                 s = 0.0;
    #                 for k in range(0, numCores):
    #                     s += response[i][k];
    #                 for k in range(0, numCores):
    #                     response[j][k] = response[j][k] - (overlap[j][i]/numStages) * ((demand[j][k]*response[i][k])/s);
    #
    #         # Calculate queue length
    #         for j in range(0, numStages):
    #             s = 0.0;
    #             for k in range(0, numCores):
    #                 s += response[j][k];
    #             for k in range(0, numCores):
    #                 Q[j][k] = response[j][k]/s;
    #
    #         # Calculate queue length at time of arrival
    #         for i in range(0, numStages):
    #             for k in range(0, numCores):
    #                 s = 0.0;
    #                 for j in range(0, numStages):
    #                     s += overlap[i][j] * Q[j][k];
    #
    #                 A[i][k] = s;
    #
    #         # Calculate response time in each center
    #         for i in range(0, numStages):
    #             for k in range(0, numCores):
    #                 response[i][k] = demand[i][k] * (1 + A[i][k]);
    #
    #         # Calculate total response time of the job
    #         for i in range(0, numStages):
    #             for k in range(0, numCores):
    #                 R_l += response[i][k];
    #
    #         # Convergence test
    #         # if abs(R_s - R_l) < tolerance:
    #         converged = 1;
    #         # else:
    #         #     R_s = R_l;
    #         #     R_l = 0.0;
    #
    #     # end of while loop
    #     return R_l
