import os, json, unittest, time, shutil, sys
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts
import argparse

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(3)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GenParity1(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()

        # always match the run below!
        # just using one file for now
        for x in [1000]:
            shCmdString = "perl " + h2o.find_file("syn_scripts/parity.pl") + " 128 4 "+ str(x) + " quad"
            h2o.spawn_cmd_and_wait('parity.pl', shCmdString.split(),4)
            csvFilename = "parity_128_4_" + str(x) + "_quad.data"  

        # always match the gen above!
        for trial in xrange (1,20,1):
            sys.stdout.write('.')
            sys.stdout.flush()

            csvFilename = "parity_128_4_" + str(1000) + "_quad.data"  
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename

            # broke out the put separately so we can iterate a test just on the RF
            key = h2o.nodes[0].put_file(csvPathname)
            parseKey = h2o.nodes[0].parse(key, key + "_" + str(trial) + ".hex")

            h2o.verboseprint("Trial", trial)
            start = time.time()
            # rfview=False used to inhibit the rfview completion
            h2o_cmd.runRFOnly(parseKey=parseKey, trees=trial, depth=2, rfview=False,
                timeoutSecs=600, retryDelaySecs=3)
            print "RF #", trial,  "started on ", csvFilename, 'took', time.time() - start, 'seconds'

            # FIX! need to get more intelligent here
            time.sleep(1)
            a = h2o.nodes[0].jobs_admin()
            print "jobs_admin():", h2o.dump_json(a)
            # "destination_key": "pytest_model", 
            # FIX! using 'key': 'pytest_model" with no time delay causes a failure
            time.sleep(1)
            jobsList = a['jobs']
            for j in jobsList:
                b = h2o.nodes[0].jobs_cancel(key=j['key'])
                print "jobs_cancel():", h2o.dump_json(b)
                # redirects to jobs, but we just do it directly.

if __name__ == '__main__':
    h2o.unit_main()

