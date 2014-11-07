import unittest, time, sys 
sys.path.extend(['.','..','../..','py'])

import h2o

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()


    def testCloud(self):
        ports_per_node = 2
        for tryNodes in range(2,8):
            sys.stdout.write('.')
            sys.stdout.flush()

            start = time.time()
            h2o.init(use_this_ip_addr="127.0.0.1", node_count=tryNodes, timeoutSecs=30, retryDelaySecs=2, java_heap_GB=1)
            print "Build cloud of %d in %d secs" % (tryNodes, (time.time() - start)) 

            h2o.verboseprint(h2o.nodes)
            h2o.verify_cloud_size()
            h2o.tear_down_cloud(h2o.nodes)

if __name__ == '__main__':
    h2o.unit_main()
