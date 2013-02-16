import os, json, unittest, time, shutil, sys, time
sys.path.extend(['.','..','py'])

import h2o_cmd, h2o, h2o_hosts
import time

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    def test_remote_Cloud(self):
        trySize = 0
        # FIX! we should increment this from 1 to N? 
        nodes_per_host = 1
        for i in range(1,10):
            # timeout wants to be larger for large numbers of hosts * nodes_per_host
            timeoutSecs = max(60, 2*(len(h2o_hosts.hosts) * nodes_per_host))
            
            h2o.build_cloud(nodes_per_host, timeoutSecs=60, retryDelaySecs=1)
            h2o_hosts.build_cloud_with_hosts(nodes_per_host, use_flatfile=True)

            h2o.verify_cloud_size()
            h2o.check_sandbox_for_errors()

            print "Tearing down cloud of size", len(h2o.nodes)
            h2o.tear_down_cloud()
            h2o.clean_sandbox()
            # wait to make sure no sticky ports or anything os-related
            # so let's expand the delay if larger number of jvms
            # 1 second per node seems good
            h2o.verboseprint("Waiting", nodes_per_host, "seconds to avoid OS sticky port problem")
            sys.stdout.write('.')
            sys.stdout.flush()
            time.sleep(nodes_per_host)


if __name__ == '__main__':
    h2o.unit_main()

