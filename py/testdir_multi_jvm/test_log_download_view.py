import unittest, time, sys
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_log

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init(3)

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

        # new logs from tom. check all are there and right size?
        # kevin@Kevin-Ubuntu4:~/h2o/py/testdir_multi_jvm/sandbox/h2ologs_20140926_111658$ unzip node0*
        # unzips to 
        # ~/h2o/py/testdir_multi_jvm/sandbox/h2ologs_20140926_111658/sandbox/ice.WkGicq/h2ologs

        # Archive:  node0_172.16.2.222_54321.zip
        #   inflating: sandbox/ice.WkGicq/h2ologs/h2o_172.16.2.222_54321-4-warn.log  
        #   inflating: sandbox/ice.WkGicq/h2ologs/h2o_172.16.2.222_54321-5-error.log  
        #   inflating: sandbox/ice.WkGicq/h2ologs/h2o_172.16.2.222_54321-httpd.log  
        #   inflating: sandbox/ice.WkGicq/h2ologs/h2o_172.16.2.222_54321-3-info.log  
        #   inflating: sandbox/ice.WkGicq/h2ologs/h2o_172.16.2.222_54321-2-debug.log  
        #   inflating: sandbox/ice.WkGicq/h2ologs/h2o_172.16.2.222_54321-6-fatal.log  
        #   inflating: sandbox/ice.WkGicq/h2ologs/h2o_172.16.2.222_54321-1-trace.log 

        (logNameList, lineCountList) = h2o_log.checkH2OLogs(timeoutSecs=180)

        self.assertEqual(len(logNameList), len(h2o.nodes), "Should be %d logs" % len(h2o.nodes))
        self.assertEqual(len(lineCountList), len(h2o.nodes), "Should be %d logs" % len(h2o.nodes))
        # line counts seem to vary..check for "too small"
        # variance in polling (cloud building and status)?
        for i, l in enumerate(lineCountList):
            self.assertGreater(l, 12, "node %d 1-trace log is too small" % i)


if __name__ == '__main__':
    h2o.unit_main()
