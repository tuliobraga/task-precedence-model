from builders.ApplicationBuilder import ApplicationBuilder
from builders.ClusterBuilder import ClusterBuilder
import sys
from bisect import insort
from hashlib import sha1
from log import readLog

#
# - Assumption 1: Spark Jobs are executed in FIFO.
#
# The appBuilder aggregates multiple Spark event logs into a single averaged Application object.
#
# Logs of the same application may differ in the amount of jobs. For example, a iterative
# process may not be deterministic. It happens, for example, in K-Means algorithm with random
# initial centroids.
#
# Because of this, the job ID in different logs may not refer to the same job.
# Therefore, we need to take care to aggregate together only identical jobs (using the jobIdentifier).
#
# Even if an specific job happens only on a single log, it must be averaged accordingly to the
# total of event logs processed. For example, suppose job ID = 13 only occurs on a single
# log file out of 10 files. When averaging this job, its length (time spent processing) must
# be divided by 10 too.
#
# In the end, the averaged Application object may not represent perfectly the job as it is
# executed in Spark. However, it will more precisely represent the set of applications the logs
# refer to uniquely. For the purposes of performance modeling, this is exactly what we need.
#
# If you need to precisely represent the computation, you should build a single log at a time.
#
def initAppFromLog(appBuilder, clusterBuilder, events):
    stageCounter = 0
    for log in events:
        if log["Event"] == "SparkListenerExecutorAdded":
            clusterBuilder.addExecutor(log["Executor ID"], log["Executor Info"]["Total Cores"])
        elif log["Event"] == "SparkListenerApplicationStart":
            appBuilder.start(log["App ID"], log["Timestamp"])
        elif log["Event"] == "SparkListenerApplicationEnd":
            appBuilder.finish(log["Timestamp"])
        elif log["Event"] == "SparkListenerJobStart":
            # the list of stages in jobs with parallel stages may be unordered
            # keeping the list sorted to guarantee equals job will match key
            keys = []
            for stg in log["Stage Infos"]:
                insort(keys, stg["Stage Name"])
            # sha1 the job key to reduce the key length - not actually required
            # if sha1() becomes a bottleneck, may be removed
            jobIdentifier = sha1(''.join(keys)).hexdigest()
            appBuilder.startJob(log["Job ID"], jobIdentifier, log["Submission Time"], log["Stage IDs"])
        elif log["Event"] == "SparkListenerJobEnd":
            appBuilder.finishJob(log["Job ID"], log["Completion Time"])
        elif log["Event"] == "SparkListenerStageSubmitted":
            stageCounter += 1
            appBuilder.startStage(log["Stage Info"]["Stage ID"], log["Stage Info"]["Parent IDs"], log["Stage Info"]["Stage Name"], log["Stage Info"]["Number of Tasks"])
        elif log["Event"] == "SparkListenerStageCompleted":
            appBuilder.finishStage(log["Stage Info"]["Stage ID"], log["Stage Info"]["Submission Time"], log["Stage Info"]["Completion Time"])
        elif log["Event"] == "SparkListenerTaskStart":
            appBuilder.startTask(log["Stage ID"], log["Task Info"]["Task ID"], log["Task Info"]["Launch Time"], int(log["Task Info"]["Executor ID"]))
        elif log["Event"] == "SparkListenerTaskEnd":
            appBuilder.finishTask(log["Stage ID"], log["Task Info"]["Task ID"], log["Task Info"]["Finish Time"])

    return stageCounter

def parseFromDir(logdir, files):
    stageCountPrev = False
    appBuilder = ApplicationBuilder()
    numCoresPrev = False

    if not files:
        print "ERROR: No log files found at %s. Please, check your parameters!" % logdir
        sys.exit()

    for file in files:
        # aggregate first and then create ApplicationBuilder
        clusterBuilder = ClusterBuilder()
        events = readLog(logdir + file)
        stageCount = initAppFromLog(appBuilder, clusterBuilder, events)

        # Validating if every log of the same scenario has the same amount of stages
        if stageCountPrev == False:
            print stageCount
            stageCountPrev = stageCount
        elif stageCount != stageCountPrev:
            print "[ERROR] A different number of stages was found between logs of the same scenario. Aborting!"
            sys.exit()

        # validating cluster config among logs
        if numCoresPrev != False:
            if numCoresPrev != clusterBuilder.getTotalCores():
                print "ERROR: Different number of cores found among replica logs.\n%s cores: File %s\n%s cores: File %s" % (str(numCoresPrev), filePrev, str(clusterBuilder.getTotalCores()), file)
                sys.exit()
            filePrev = file
        else:
            numCoresPrev = clusterBuilder.build().getTotalCores()
            filePrev = file

    cluster = clusterBuilder.build()
    app = appBuilder.build()
    return app, cluster
