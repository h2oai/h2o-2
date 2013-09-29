import unittest, time, sys
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_import as h2i, h2o_jobs

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

    def test_1ktrees_job_cancel_many(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()

        # always match the run below!
        # just using one file for now
        for x in [1000]:
            shCmdString = "perl " + h2o.find_file("syn_scripts/parity.pl") + " 128 4 "+ str(x) + " quad"
            h2o.spawn_cmd_and_wait('parity.pl', shCmdString.split(),4)
            csvFilename = "parity_128_4_" + str(x) + "_quad.data"  

        csvFilename = "parity_128_4_" + str(1000) + "_quad.data"  
        csvPathname = SYNDATASETS_DIR + '/' + csvFilename
        hex_key = csvFilename + ".hex"
        parseResult = h2o_cmd.parseResult = h2i.import_parse(path=csvPathname, schema='put', hex_key=hex_key, timeoutSecs=30)

        print "Kick off twenty, then cancel them all..there's a timeout on the wait after cancelling"
        for trial in range (1,20):
            h2o.verboseprint("Trial", trial)
            start = time.time()
            h2o_cmd.runRF(parseResult=parseResult, trees=trial, depth=50, rfView=False, noPoll=True,
                timeoutSecs=600, retryDelaySecs=3)
            print "RF #", trial,  "started on ", csvFilename, 'took', time.time() - start, 'seconds'


        h2o.check_sandbox_for_errors()
        h2o_jobs.cancelAllJobs(timeoutSecs=10)


if __name__ == '__main__':
    h2o.unit_main()

