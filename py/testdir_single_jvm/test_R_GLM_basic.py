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

    def test_R_B_benign(self):
        print "\nStarting benign.csv"
        rScript = h2o.find_file('R/tests/test_R_B_benign.R')
        rLibrary = h2o.find_file('R/H2O_Load.R')

        # Columns start at 0
        # Test columns 0-13, with 3 as response
        # N-fold cross-validation = 5
        h2o_R.do_R(rScript, rLibrary)

    def test_R_C_prostate(self):
        print "\nStarting prostate.csv"
        rScript = h2o.find_file('R/tests/test_R_C_prostate.R')
        rLibrary = h2o.find_file('R/H2O_Load.R')

        # Columns start at 0
        # Test columns 1-8, with 1 as response
        # (Skip 0 because member ID)
        h2o_R.do_R(rScript, rLibrary)

if __name__ == '__main__':
    h2o.unit_main()
