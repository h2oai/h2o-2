import unittest, random, sys, time, os
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import as h2i, h2o_exec as h2e

zeroList = [
        'Result0 = c(0)',
        'Result1 = c(0) + Result0',
        'Result2 = c(0) + Result1',
        'Result3 = c(0) + Result2',
        'Result4 = rep_len(0,1000000)',
        'Result5 = rep_len(0,1000000) + Result4',
        'Result6 = rep_len(0,1000000) + Result5',
        'Result7 = rep_len(0,1000000) + Result6',
        'Result8 = rep_len(0,1000000) + Result7',
        'Result9 = rep_len(0,1000000) + Result8',
        'Result.hex = c(0)',
]

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED, localhost
        SEED = h2o.setup_random_seed()
        localhost = h2o.decide_if_localhost()
        h2o.init(3,java_heap_GB=4)

    @classmethod
    def tearDownClass(cls):
        # wait while I inspect things
        # time.sleep(1500)
        h2o.tear_down_cloud()

    def test_exec2_constants(self):
        print "Create some vectors from a constant"
        print "Don't really need a dataset, but .."
        for i in range(10):
            h2e.exec_zero_list(zeroList)
            inspect = h2o_cmd.runInspect(key='Result9')
            h2o_cmd.infoFromInspect(inspect, 'Result9')
            numRows = inspect['numRows']
            numCols = inspect['numCols']
            self.assertEqual(numRows, 1000000)
            self.assertEqual(numCols, 1)

if __name__ == '__main__':
    h2o.unit_main()
