import os, json, unittest, time, shutil, sys
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts

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

    def test_B_hhp_107_01_loop(self):
        timeoutSecs = 10
        trial = 1
        n = h2o.nodes[0]
        for x in xrange (1,30,1):
            sys.stdout.write('.')
            sys.stdout.flush()

            csvPathname = h2o.find_file("smalldata/hhp_107_01.data.gz")
            key = n.put_file(csvPathname)
            parseKey = n.parse(key, key + "_" + str(x) + ".hex")

            ### print 'Trial:', trial
            trial += 1

if __name__ == '__main__':
    h2o.unit_main()
