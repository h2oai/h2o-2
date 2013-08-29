import unittest, time, sys
sys.path.extend(['.','..','py'])
import h2o, h2o_hosts, h2o_R

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
        # shouldn't need this...placeholder for experiment
        h2o.kill_child_processes()
        h2o.tear_down_cloud()

    def test_R_RF_diff_class(self):
        print "\nStarting iris.csv class weight test"
        rScript = h2o.find_file('R/tests/test_R_RF_diff_class.R')
        rLibrary = h2o.find_file('R/H2O_Load.R')

        # Compare results from different class weights
        h2o_R.do_R(rScript, rLibrary)

    def test_R_RF_diff_ignore(self):
        print "\nStarting iris.csv ignore predictor(s) test"
        rScript = h2o.find_file('R/tests/test_R_RF_diff_ignore.R')
        rLibrary = h2o.find_file('R/H2O_Load.R')

        # Ignore successively more predictor columns
        h2o_R.do_R(rScript, rLibrary)

if __name__ == '__main__':
    h2o.unit_main()
    
