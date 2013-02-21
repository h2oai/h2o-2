import os, json, unittest, time, shutil, sys, time
sys.path.extend(['.','..','py'])

import h2o_cmd, h2o, h2o_hosts
import time

node_count = 16
base_port = 54321
class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        # do the first one to build up hosts
        # so we don't repeatedly copy the jar
        # have to make sure base_port is the same on both!
        h2o.write_flatfile(node_count=node_count, base_port=base_port)
        h2o_hosts.build_cloud_with_hosts(node_count, base_port=base_port, use_flatfile=True)
        h2o.verify_cloud_size()
        h2o.check_sandbox_for_errors()

    @classmethod
    def tearDownClass(cls):
        pass

    def test_remote_Cloud(self):
        trySize = 0
        # FIX! we should increment this from 1 to N? 
        for i in range(1,10):
            # timeout wants to be larger for large numbers of hosts * node_count
            # don't want to reload jar/flatfile, so use build_cloud
            timeoutSecs = max(60, 2*(len(h2o_hosts.hosts) * node_count))
            h2o.build_cloud(node_count, hosts=h2o_hosts.hosts, use_flatfile=True, 
                timeoutSecs=timeoutSecs, retryDelaySecs=0.5)
            h2o.verify_cloud_size()
            h2o.check_sandbox_for_errors()

            print "Tearing down cloud of size", len(h2o.nodes)
            h2o.tear_down_cloud()
            h2o.clean_sandbox()
            # wait to make sure no sticky ports or anything os-related
            # so let's expand the delay if larger number of jvms
            # 1 second per node seems good
            h2o.verboseprint("Waiting", node_count, "seconds to avoid OS sticky port problem")
            sys.stdout.write('.')
            sys.stdout.flush()
            time.sleep(node_count)


if __name__ == '__main__':
    h2o.unit_main()

