import unittest, time, sys
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_util, h2o_log

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init(3)

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
