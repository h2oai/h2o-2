import os, json, unittest, time, shutil, sys
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd
import h2o_hosts, h2o_glm
import h2o_browse as h2b
import time

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        localhost = h2o.decide_if_localhost()
        if (localhost):
            # maybe fails more reliably with just 2 jvms?
            h2o.build_cloud(2,java_heap_GB=5)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_C_hhp_107_01(self):
        csvPathname = h2o.find_file("smalldata/hhp_107_01.data.gz")
        print "\n" + csvPathname
        parseKey = h2o_cmd.parseFile(csvPathname=csvPathname, timeoutSecs=15)

        # pop open a browser on the cloud
        h2b.browseTheCloud()

        # build up the parameter string in X
        y = "106"
        x = ""

        # go right to the big X and iterate on that case
        ### for trial in range(2):
        for trial in range(2):
            print "\nTrial #", trial, "start"
            print "\nx:", x
            print "y:", y

            start = time.time()
            kwargs = {'y': y}
            glm = h2o_cmd.runGLMOnly(parseKey=parseKey, timeoutSecs=200, **kwargs)
            h2o_glm.simpleCheckGLM(self, glm, 57, **kwargs)
            h2o.check_sandbox_for_errors()
            ### h2b.browseJsonHistoryAsUrlLastMatch("GLM")
            print "\nTrial #", trial


if __name__ == '__main__':
    h2o.unit_main()
