import os, json, unittest, time, shutil, sys
sys.path.extend(['.','..','py'])

# auto-ignores constant columns? fails with exception below in browser
# ignoring constant column 9
# ignoring constant column 12
# ignoring constant column 25
# ignoring constant column 54
# ignoring constant column 76
# ignoring constant column 91
# ignoring constant column 103

import h2o, h2o_cmd, h2o_hosts, h2o_glm
import h2o_browse as h2b
import time

# can expand this with specific combinations
# I suppose these args will be ignored with old??
argcaseList = [
# FIX! we get stack trace if we specify a column that was dropped because it's constant
# for instance, column 9
###    {   'x': '0,1,2,3,4,5,6,7,8,9,10,11',
    {   'x': '0,1,2,3,4,5,6,7,8,10,11',
        'y': 106,
        'family': 'gaussian',
        'lambda': 1.0E-5,
        'max_iter': 50,
        'weight': 1.0,
        'thresholds': 0.5,
        'link': 'familyDefault',
        'num_cross_validation_folds': 0,
        'alpha': 1,
        'beta_epsilon': 1.0E-4 },
    ]

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(1,java_heap_GB=1)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_hhp_107_01(self):
        csvPathname = h2o.find_file("smalldata/hhp_107_01.data.gz")
        print "\n" + csvPathname
        parseKey = h2o_cmd.parseFile(csvPathname=csvPathname, key2="hhp_107_01.data.hex", timeoutSecs=15)

        # pop open a browser on the cloud
        h2b.browseTheCloud()
        trial = 0
        for argcase in argcaseList:
            print "\nTrial #", trial, "start"
            kwargs = argcase
            print 'y:', kwargs['y']
            start = time.time()
            glm = h2o_cmd.runGLMOnly(parseKey=parseKey, browseAlso=True, timeoutSecs=200, **kwargs)
            h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)

            print "\nTrial #", trial


if __name__ == '__main__':
    h2o.unit_main()
