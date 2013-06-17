import os, json, unittest, time, shutil, sys
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts, h2o_util, h2o_log

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

    def test_log_download_terminate(self):
        csvPathnamegz = h2o.find_file('smalldata/hhp_9_17_12.predict.data.gz')
        h2o_cmd.runRF(trees=6, timeoutSecs=30, csvPathname=csvPathnamegz)

        # download and view using each node
        for h in h2o.nodes:
            h.log_view()
            h.log_download(timeoutSecs=3)

        # terminate node 1
        h2o.nodes[1].terminate_self_only()

        # wait to make sure heartbeat updates cloud status
        time.sleep(5)

        print "Checking cloud size at nodes 0 and 2 after terminating 1"
        nodesNow = [h2o.nodes[0], h2o.nodes[2]]
        expectedSize = len(nodesNow)
        cloudSizes = [n.get_cloud()['cloud_size'] for n in nodesNow]
        cloudConsensus = [n.get_cloud()['consensus'] for n in nodesNow]
        for s in cloudSizes:
            consensusStr = (",".join(map(str,cloudConsensus)))
            sizeStr =   (",".join(map(str,cloudSizes)))
            if (s != expectedSize):
                raise Exception("Inconsistent cloud size." +
                    "nodes report size: %s consensus: %s instead of %d." % \
                    (sizeStr, consensusStr, expectedSize))

        (logNameList, lineCountList) = h2o_log.checkH2OLogs()
        nodesNum = len(h2o.nodes)
        self.assertEqual(len(logNameList), nodesNum, "Should be " + nodesNum + " logs")
        self.assertEqual(len(lineCountList), nodesNum, "Should be " + nodesNum + " logs")

        for i in range(nodesNum):
            # line counts seem to vary..check for "too small"
            # variance in polling (cloud building and status)?
            self.assertGreater(lineCountList[i], 12, "node " + str(i) + " log is too small")

if __name__ == '__main__':
    h2o.unit_main()
