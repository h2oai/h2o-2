
import h2o, h2o_util
import re

def checkH2OLogs(timeoutSecs=3):
    # download and view using each node, just to see we can
    # each overwrites
    for h in h2o.nodes:
        h.log_view()
        h.log_download(timeoutSecs=timeoutSecs)

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
    return (logNameList, lineCountList)
