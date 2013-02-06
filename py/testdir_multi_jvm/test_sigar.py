import os, json, unittest, time, shutil, sys
sys.path.extend(['.','..','py'])

import h2o

class SigarApi(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.clean_sandbox()
        h2o.build_cloud(3,sigar=True)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_netstat(self):
        # Ask each node for network statistics
        for n in h2o.nodes:
            a = n.netstat()
            print a

if __name__ == '__main__':
    unittest.main()
