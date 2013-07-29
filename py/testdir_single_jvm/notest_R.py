import unittest, time, sys
sys.path.extend(['.','..','py'])

import h2o, h2o_hosts

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

    def test_R(self):
        rScript = h2o.find_file('R/tests/test_R_GLM_basic.R')
        rLibrary = h2o.find_file('R/H2O_S4.R')
        shCmdString = "R -f " + rScript + " --args " + rLibrary + " " + h2o.nodes[0].http_addr + ":" + str(h2o.nodes[0].port)

        (ps, outpath, errpath) =  h2o.spawn_cmd('rtest_with_h2o', shCmdString.split())
        h2o.spawn_wait(ps, outpath, errpath, timeout=10)

if __name__ == '__main__':
    h2o.unit_main()
