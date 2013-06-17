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

    def test_log_download_view(self):
        (logNameList, lineCountList) = h2o_log.checkH2OLogs()

        self.assertEqual(len(logNameList), 3, "Should be 3 logs")
        self.assertEqual(len(lineCountList), 3, "Should be 3 logs")
        # line counts seem to vary..check for "too small"
        # variance in polling (cloud building and status)?
        self.assertGreater(lineCountList[0], 12, "node 0 log is too small")
        self.assertGreater(lineCountList[1], 12, "node 1 log is too small")
        self.assertGreater(lineCountList[2], 12, "node 2 log is too small")


if __name__ == '__main__':
    h2o.unit_main()
