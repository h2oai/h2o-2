import unittest, time, sys
sys.path.extend(['.','..','../..','py'])

import h2o

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def tearDownClass(cls):
        # this is for safety after error, plus gets us the grep of stdout/stderr for errors
        h2o.tear_down_cloud()

    def test_Cloud(self):
        # FIX! weird timeout H2O exceptions with >8? maybe shouldn't
        # don't know if we care
        ports_per_node = 2
        for tryNodes in range(17,25):
            h2o.verboseprint("Trying cloud of", tryNodes)
            sys.stdout.write('.')
            sys.stdout.flush()

            start = time.time()
            h2o.init(tryNodes, retryDelaySecs=2, timeoutSecs=max(30,10*tryNodes), java_heap_GB=1)
            print "Built cloud of %d in %d s" % (tryNodes, (time.time() - start)) 
            h2o.verify_cloud_size()
            h2o.tear_down_cloud()
if __name__ == '__main__':
    h2o.unit_main()
