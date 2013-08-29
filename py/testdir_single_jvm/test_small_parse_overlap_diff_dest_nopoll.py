import sys, unittest, random, time
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_jobs, h2o_import2 as h2i 

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(1, java_heap_GB=10)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod 
    def tearDownClass(cls): 
        h2o.tear_down_cloud()
 
    def test_small_parse_overlap_same_dest(self):
        noPoll = True
        timeoutSecs = 180
        num_trials = 0
        stallForNJobs = 100
        for i in range(50):
            for j in range(200):
                csvFilename = 'poker-hand-testing.data'
                csvPathname = 'poker/' + csvFilename
                src_key = csvFilename + "_" + str(i) + "_" + str(j)
                hex_key =  csvFilename + "_" + str(num_trials) + '.hex'
                parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, schema='put',
                    src_key=src_key, hex_key=hex_key, timeoutSecs=timeoutSecs, noPoll=noPoll,
                    doSummary=False)
                num_trials += 1
            h2o_jobs.pollWaitJobs(timeoutSecs=timeoutSecs, pollTimeoutSecs=120, retryDelaySecs=5,stallForNJobs=stallForNJobs)

if __name__ == "__main__":
    h2o.unit_main()
