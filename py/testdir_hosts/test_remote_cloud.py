import os, json, unittest, time, shutil, sys, time
sys.path.extend(['.','..','py'])

import h2o_cmd, h2o, h2o_hosts
import time

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        # this cloud build will use the json 
        # and setup hosts with ip, and username/password, 
        print "Setup for first cloud"
        h2o_hosts.build_cloud_with_hosts()
        h2o.verify_cloud_size()
        print "\nTearing down first cloud"
        h2o.tear_down_cloud()
        h2o.clean_sandbox()

    @classmethod
    def tearDownClass(cls):
        pass

    def test_remote_Cloud(self):
        trySize = 0
        # FIX! we should increment this from 1 to N? 
        nodes_per_host = 1
        for i in range(1,10):
            sys.stdout.write('.')
            sys.stdout.flush()

            # nodes_per_host is per host.
            # timeout wants to be larger for large numbers of hosts * nodes_per_host
            # use 60 sec min, 2 sec per node.
            timeoutSecs = max(60, 2*(len(h2o_hosts.hosts) * nodes_per_host))
            
            h2o.build_cloud(nodes_per_host, hosts=h2o_hosts.hosts, timeoutSecs=60, retryDelaySecs=1)
            h2o.verify_cloud_size()
            trySize += 1
            # FIX! is this really needed? does tear_down_cloud do shutdown command and wait?
            print "Sending shutdown to cloud, trySize", trySize
            h2o.nodes[0].shutdown_all()
            time.sleep(1)

            print "Tearing down cloud, trySize", trySize
            sys.stdout.write('.')
            sys.stdout.flush()
            h2o.tear_down_cloud()
            h2o.clean_sandbox()
            # wait to make sure no sticky ports or anything os-related
            # here's a typical error that shows up in the looping trySizes..
            # so let's expand the delay if larger number of jvms
            # don't need so much delay for smaller..all os issues.

            # On /192.168.1.156 some of the required ports 56339, 56340, and 56341 
            # are not available, change -port PORT and try again.

            # 1 second per node seems good
            h2o.verboseprint("Waiting", nodes_per_host, "seconds to avoid OS sticky port problem")
            time.sleep(nodes_per_host)


if __name__ == '__main__':
    h2o.unit_main()

