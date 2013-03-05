import os, json, unittest, time, shutil, sys, time
sys.path.extend(['.','..','py'])

import h2o_cmd, h2o, h2o_hosts
import time

node_count = 10
base_port = 54321
class Basic(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        start = time.time()
        h2o_hosts.build_cloud_with_hosts(node_count, base_port=base_port, 
            use_flatfile=True, java_heap_GB=1)
        print "Cloud of", len(h2o.nodes), "built in", time.time()-start, "seconds"
        h2o.verify_cloud_size()
        h2o.check_sandbox_for_errors()

    @classmethod
    def tearDownClass(cls):
        pass

    def test_remote_cloud1(self):
        pass
     

if __name__ == '__main__':
    h2o.unit_main()

