# read log line by line as json
def readLog(logpath):
	import json
	return [json.loads(line.replace("\r", "").replace("\n", "")) for line in tuple(open(logpath, 'r'))]

def loadLogs(logpath):
	import os
	return [s for s in os.listdir(logpath) if not s.startswith('.DS_Store') and not s.endswith('.zip') and not os.path.isdir(logpath + s)]
