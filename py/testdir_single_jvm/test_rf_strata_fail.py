import unittest
import random, sys
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_rf, h2o_hosts

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(node_count=1)
        else:
            h2o_hosts.build_cloud_with_hosts(node_count=1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_rf_strata_fail(self):
        csvPathname = h2o.find_file('smalldata/poker/poker1000')
        for trial in range(20):
            # params is mutable. This is default.
            kwargs = {
                'sampling_strategy': 'STRATIFIED_LOCAL', 
                'out_of_bag_error_estimate': 1, 
                'strata_samples': '0=0,1=0,2=0,3=0,4=0,5=0,6=0,7=0,8=0', 
                'ntree': 19, 
                'parallel': 1
            }
            timeoutSecs = 10
            h2o_cmd.runRF(timeoutSecs=timeoutSecs, csvPathname=csvPathname, **kwargs)
            print "Trial #", trial, "completed"

if __name__ == '__main__':
    h2o.unit_main()
