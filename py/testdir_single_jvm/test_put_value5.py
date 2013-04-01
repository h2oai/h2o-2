import os, json, unittest, time, shutil, sys
sys.path.extend(['.','..','py'])

import h2o

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

    def test1(self):
        for x in xrange (1,2000,1):
            if ((x % 100) == 0):
                sys.stdout.write('.')
                sys.stdout.flush()

            trialString = "Trial" + str(x)
            trialStringXYZ = "Trial" + str(x) + "XYZ"
            put = h2o.nodes[0].put_value(trialString, key=trialStringXYZ, repl=None)

if __name__ == '__main__':
    h2o.unit_main()
