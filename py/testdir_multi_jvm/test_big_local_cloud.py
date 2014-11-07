import unittest, time, sys
sys.path.extend(['.','..','../..','py'])

import h2o
import time

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    def test_Cloud(self):
        ports_per_node = 2
        for trials in range(0,5):
            for tryNodes in range(3,6):
                sys.stdout.write('.')
                sys.stdout.flush()

                start = time.time()
                # start by cleaning sandbox (in build_cloud). 
                # so nosetest works which doesn't do unit_main

                h2o.init(node_count=tryNodes, java_heap_GB=1, timeoutSecs=30, retryDelaySecs=2, use_flatfile=True)
                print "loop %d: Build cloud of %d in %d s" % (trials, tryNodes, (time.time() - start)) 

                for i in range(2):
                    print "nodes report size: %s consensus: %s expected: %d." % h2o.verify_cloud_size()

                h2o.tear_down_cloud()
                # with so many jvms, wait for sticky ports to be freed up..slow os stuff?
                # changed, to increment the base_port, to avoid reuse immediately
                time.sleep(1)
                base_port += ports_per_node * tryNodes


if __name__ == '__main__':
    h2o.unit_main()
