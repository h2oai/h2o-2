#!/usr/bin/python
import unittest, time, sys, random
sys.path.extend(['.','..','py','../h2o/py','../../h2o/py'])
import h2o, h2o_cmd

start = time.time()

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):

        global SEED, localhost
        SEED = h2o.setup_random_seed()
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(3, create_json=True, java_heap_GB=4)
        else:
            h2o_hosts.build_cloud_with_hosts(create_json=True)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_build_for_clone(self):
        elapsed = time.time() - start
        print "\n%0.2f seconds to get here from start" % elapsed

        maxTime = 4*3600
        totalTime = 0
        incrTime = 60
        print "\nSleeping for total of", (maxTime+0.0)/3600, "hours." 
        print "Will check h2o logs every", incrTime, "seconds"
        print "Should be able to run another test using h2o-nodes.json to clone cloud"
        print "i.e. h2o.build_cloud_with_json()"
        print "Error if a running test shuts down the cloud. I'm supposed to!"
        while (totalTime<maxTime): # die after 4 hours
            h2o.sleep(incrTime)
            totalTime += incrTime
            # good to touch all the nodes to see if they're still responsive
            # give them up to 120 secs to respond (each individually)
            h2o.verify_cloud_size(timeoutSecs=120)
            print "Checking sandbox log files"
            h2o.check_sandbox_for_errors(cloudShutdownIsError=True)

        start2 = time.time()
        h2i.delete_keys_at_all_nodes()
        elapsed = time.time() - start2
        print "delete_keys_at_all_nodes(): took", elapsed, "secs"

if __name__ == '__main__':
    h2o.unit_main()
