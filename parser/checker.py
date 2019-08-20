from log import readLog

def check(app, cluster, logdir, files):
    numCores = cluster.getTotalCores()
    print "App end - start: " + str(app._endTime - app._startTime)
    print "App length: " + str(app._applicationLength)
    print "Exec length: " + str(app._executionLength)

    jl = 0
    sl = 0
    tl = 0
    for j in app._jobs:
        jl += j._length
        for sid in j._stages:
            s = j._stages[sid]
            sl += s._length
            st = 0
            for t in s._tasks:
                st += t._length
            st /= len(s._tasks)
            reminder = len(s._tasks) % numCores
            extra = 1 if reminder > 0 else 0
            queueLength = (len(s._tasks) / numCores) + extra
            tl += queueLength*st

    print "Sum of jobs length: " + str(jl)
    print "Sum of stages length: " + str(sl)
    print "Sum of tasks length considering number of vCPUs: " + str(tl)

    sum = 0
    for file in files:
        events = readLog(logdir + file)
        for log in events:
            if log["Event"] == "SparkListenerApplicationStart":
                start = log["Timestamp"]
            elif log["Event"] == "SparkListenerApplicationEnd":
                end = log["Timestamp"]

        sum += end-start
    print 'Avg. app end - start: ' + str(sum/len(files))
