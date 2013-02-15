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
        print "\nTearing down first cloud"
        h2o.tear_down_cloud()
        h2o.clean_sandbox()

    @classmethod
    def tearDownClass(cls):
        pass

    def test_remote_Cloud(self):
        trial = 0
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

            # FIX! if node[0] is fast, maybe the other nodes aren't at a point where they won't get
            # connection errors. Stabilize them too! Can have short timeout here, because they should be 
            # stable?? or close??

            # FIX! using "consensus" in node[0] should mean this is unnecessary?
            # maybe there's a bug
            for n in h2o.nodes:
                h2o.stabilize_cloud(n, len(h2o.nodes), timeoutSecs=30, retryDelaySecs=1)

            # now double check ...no stabilize tolerance of connection errors here
            for n in h2o.nodes:
                print "Checking n:", n
                c = n.get_cloud()
                self.assertFalse(c['cloud_size'] > len(h2o.nodes), 'More nodes than we want. Zombie JVMs to kill?')
                self.assertEqual(c['cloud_size'], len(h2o.nodes), 'inconsistent cloud size')

            trial += 1
            # FIX! is this really needed? does tear_down_cloud do shutdown command and wait?
            print "Sending shutdown to cloud, trial", trial
            h2o.nodes[0].shutdown_all()
            time.sleep(1)

            print "Tearing down cloud, trial", trial
            sys.stdout.write('.')
            sys.stdout.flush()
            h2o.tear_down_cloud()
            h2o.clean_sandbox()
            # wait to make sure no sticky ports or anything os-related
            # here's a typical error that shows up in the looping trials..
            # so let's expand the delay if larger number of jvms
            # don't need so much delay for smaller..all os issues.

            # On /192.168.1.156 some of the required ports 56339, 56340, and 56341 
            # are not available, change -port PORT and try again.

            # 1 second per node seems good
            h2o.verboseprint("Waiting", nodes_per_host, "seconds to avoid OS sticky port problem")
            time.sleep(nodes_per_host)


if __name__ == '__main__':
    h2o.unit_main()

