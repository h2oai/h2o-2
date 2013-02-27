import os, json, unittest, time, shutil, sys 
sys.path.extend(['.','..','py'])

import h2o
import h2o_browse as h2b


class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    def testCloud(self):
        baseport = 54300
        ports_per_node = 2

        print "\nTest was written because seeing a bigger cloud than we want sometimes"
        print "You'll see the problem in the cloud in the browser"
        print "\nWorks if real ip address used. fails with 127.0.0.1 (intermittent)"
        print "Builds cloud with 3, the extra being a non-127.0.0.1 node (the real ip)"
        print "Eventually it goes away, around 1 minute?"
        for trial in range(20):
            for tryNodes in range(2,3):
                sys.stdout.write('.')
                sys.stdout.flush()

                start = time.time()
                ### this works
                ### h2o.build_cloud(use_this_ip_addr="192.168.0.37",
                # this intermittently fails
                h2o.build_cloud(use_this_ip_addr="127.0.0.1", 
                    node_count=tryNodes, base_port=base_port, java_heap_GB=1,
                    timeoutSecs=15, retryDelaySecs=2)
                print "trial #%d: Build cloud of %d in %d secs" % (trial, tryNodes, (time.time() - start)) 

                h2o.verify_cloud_size()
                h2o.tear_down_cloud()

                # increment the base_port to avoid sticky ports when we do another
                # we only use two ports now?
                baseport += ports_per_node * tryNodes

if __name__ == '__main__':
    h2o.unit_main()
