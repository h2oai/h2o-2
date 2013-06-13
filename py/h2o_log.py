
import h2o, h2o_util

def checkH2OLogs(timeoutSecs=3):
    # download and view using each node, just to see we can
    # each overwrites
    for h in h2o.nodes:
        h.log_view()
        h.log_download(timeoutSecs=timeoutSecs)

    # download logs from node 0 (this will overwrite)
    h2o.nodes[0].log_download(timeoutSecs=timeoutSecs)
    # figure out the expected log file names (node/ip/port)
    # node0_192.168.1.171_54326.log
    # need to get a list of node names in the order H2O is using
    # since the logs have prefix node0 thru nodeN
    nodes = h2o.nodes[0].get_cloud()['nodes']
    nodeNameList = [i['name'] for i in nodes]
    logNameList = []
    lineCountList = []
    for i,nodeName in enumerate (nodeNameList):
        # what order are the nodes in? (which is node0?)
        print "nodeName:", nodeName
        # node_name: /192.168.1.28:54323
        nodeName = nodeName.replace(":","_")
        nodeName = nodeName.replace("/","_")
        logName = "node" + str(i) + nodeName + ".log"

        # logName = "node" + str(i) + "_" + str(node.http_addr) + "_" + str(node.port) + ".log"
        lineCount = h2o_util.file_line_count("sandbox/" + logName)
        print logName, "lineCount:", lineCount
        logNameList.append(logName)
        lineCountList.append(lineCount)

    print logNameList
    return (logNameList, lineCountList)
