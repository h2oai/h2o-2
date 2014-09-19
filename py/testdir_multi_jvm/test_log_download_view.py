import unittest, time, sys
sys.path.extend(['.','..','py'])
import h2o, h2o_hosts, h2o_log

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
        # h2o.sleep(3600)
        h2o.tear_down_cloud()

    def test_log_download_view(self):
        # download and view using each node, just to see we can
        # each overwrites
        for h in h2o.nodes:
            h.log_view()
            # checkH2OLogs will download
            # h.log_download(timeoutSecs=5)

        # this gets them all thru node 0
        (logNameList, lineCountList) = h2o_log.checkH2OLogs(timeoutSecs=180)

        self.assertEqual(len(logNameList), len(h2o.nodes), "Should be %d logs" % len(h2o.nodes))
        self.assertEqual(len(lineCountList), len(h2o.nodes), "Should be %d logs" % len(h2o.nodes))
        # line counts seem to vary..check for "too small"
        # variance in polling (cloud building and status)?
        for i, l in enumerate(lineCountList):
            self.assertGreater(l, 12, "node %d log is too small" % i)


if __name__ == '__main__':
    h2o.unit_main()
