import os, json, unittest, time, shutil, sys
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts, h2o_util

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(3)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_log_download_terminate2(self):

        # download and view using each node
        for h in h2o.nodes:
            h.log_view()
            h.log_download(timeoutSecs=3)

        # terminate node 1
        h2o.nodes[1].terminate_self_only()

        # wait to make sure heartbeat updates cloud status
        time.sleep(5)

        # download logs from node 0
        h2o.nodes[0].log_download(timeoutSecs=3)
        # figure out the expected log file names (node/ip/port)
        # node0_192.168.1.171_54326.log
        logNames = []
        for i,node in enumerate(h2o.nodes):
            lineCount = h2o_util.file_line_count("sandbox/" + logName)
            logName = "node" + str(i) + node.addr + "_" + node.port
            logNames.append(logName)
            print logName, "lineCount:", lineCount

        print logNames

if __name__ == '__main__':
    h2o.unit_main()
