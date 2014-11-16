import unittest, time, sys
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_import as h2i

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init(3)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_rf_1ktrees_job_cancel_3_fvec(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()

        # always match the run below!
        # just using one file for now
        for x in [1000]:
            shCmdString = "perl " + h2o.find_file("syn_scripts/parity.pl") + " 128 4 "+ str(x) + " quad " + SYNDATASETS_DIR
            h2o.spawn_cmd_and_wait('parity.pl', shCmdString.split(),4)
            csvFilename = "parity_128_4_" + str(x) + "_quad.data"  

        # always match the gen above!
        for trial in range (1,20):
            sys.stdout.write('.')
            sys.stdout.flush()

            csvFilename = "parity_128_4_" + str(1000) + "_quad.data"  
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename

            hex_key = csvFilename + "_" + str(trial) + ".hex"
            parseResult = h2o_cmd.parseResult = h2i.import_parse(path=csvPathname, schema='put', hex_key=hex_key, timeoutSecs=30)

            h2o.verboseprint("Trial", trial)
            start = time.time()
            h2o_cmd.runRF(parseResult=parseResult, trees=trial, max_depth=2, rfView=False,
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

