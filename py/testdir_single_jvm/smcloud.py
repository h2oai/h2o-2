#!/usr/local/bin/python2.7
import unittest, time, sys
sys.path.extend(['.','..','py'])
import h2o_cmd, h2o, h2o_hosts, h2o_browse as h2b

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        # Uses your username specific json: pytest_config-<username>.json
        global localhost
        h2o.beta_features = True
        # localhost = h2o.decide_if_localhost()
        # if localhost:
        if 1==0:
            h2o.build_cloud(1, use_hdfs=True, 
            hdfs_name_node='10.71.0.100', hdfs_version='cdh5',
            java_heap_GB=4, base_port=54321)
        else:
            h2o.config_json='/home/0xdiag/h2o/py/testdir_hosts/pytest_config-sm32.json'
            h2o.verbose = True
            h2o_hosts.build_cloud_with_hosts(1, base_port=54321)
            h2o.verbose = False

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_smcloud(self):
        if not h2o.browse_disable:
            time.sleep(500000)

if __name__ == '__main__':

    h2o.unit_main()
