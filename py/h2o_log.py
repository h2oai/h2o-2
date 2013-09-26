
import h2o, h2o_util
import re

def checkH2OLogs(timeoutSecs=3, expectedMinLines=12):
    # download logs from node 0 (this will overwrite)
    h2o.nodes[0].log_download(timeoutSecs=timeoutSecs)

    # I guess we really don't need to get the list of nodes names from get_cloud any more
    logNameList = ["h2o_" + str(n.http_addr) + "_" + str(n.port) + ".log" for n in h2o.nodes]
    lineCountList = []
    for logName in logNameList:
        lineCount = h2o_util.file_line_count("sandbox/" + logName)
        print logName, "lineCount:", lineCount
        lineCountList.append(lineCount)

    print logNameList

    if len(h2o.nodes) != len(logNameList):
        raise Exception("Should be %d logs, are %d" % len(h2o.nodes), len(logNameList))

    # line counts seem to vary..check for "too small"
    # variance in polling (cloud building and status)?
    for i, l in enumerate(lineCountList):
        if l < expectedMinLines:
            raise Exception("node %d log is too small" % i)

    # now that all the logs are there
    h2o.check_sandbox_for_errrs()
    return (logNameList, lineCountList)

def getH2OScripts(timeoutSecs=30):
    # download and view using each node, just to see we can
    # each overwrites
    scriptNameList = []
    lineCountList = []
    for i,h in enumerate(h2o.nodes):
        pathname = "sandbox/script_" + str(i) + ".txt"
        h.script_download(pathname, timeoutSecs)
        scriptNameList.append(pathname)

    for scriptName in scriptNameList:
        lineCount = h2o_util.file_line_count(scriptName)
        print scriptName, "lineCount:", lineCount
        lineCountList.append(lineCount)

    print scriptNameList
    return (scriptNameList, lineCountList)
