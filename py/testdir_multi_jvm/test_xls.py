import os, json, unittest, time, shutil, sys
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts

class TestExcel(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(3,java_heap_MB=1300)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_iris_xls(self):
        h2o_cmd.runRF(None, h2o.find_dataset('iris/iris.xls'), timeoutSecs=5)

    def test_iris_xlsx(self):
        h2o_cmd.runRF(None, h2o.find_dataset('iris/iris.xlsx'), timeoutSecs=5)

    def test_poker_xls(self):
        h2o_cmd.runRF(None, h2o.find_dataset('poker/poker-hand-testing.xls'), timeoutSecs=10)

    def test_poker_xlsx(self):
        # maybe can get stuck during polling for parse progress?
        # break it out for pollTimeoutSecs
        parseKey = h2o_cmd.parseFile(None, h2o.find_dataset('poker/poker-hand-testing.xlsx'), 
            timeoutSecs=120, pollTimeoutSecs=60)
        h2o_cmd.runRFOnly(None, parseKey=parseKey, timeoutSecs=120)

if __name__ == '__main__':
    h2o.unit_main()
