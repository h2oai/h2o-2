import os, json, unittest, time, shutil, sys, time
sys.path.extend(['.','..','py'])

import h2o_cmd, h2o, h2o_hosts
import time

node_count = 8
base_port = 54321
class Basic(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # do the first one to build up hosts
        # so we don't repeatedly copy the jar
        # have to make sure base_port is the same on both!
        # just do once and don't clean sandbox
        h2o.write_flatfile(node_count=node_count, base_port=base_port)
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

