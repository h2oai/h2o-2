import sys, unittest, random, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_jobs, h2o_import as h2i 

print "not really getting any overlap because the parse is very fast for the small file..."
print "not really sure if we care about trying to get more overlap"

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors(sandboxIgnoreErrors=True)

    @classmethod
    def setUpClass(cls):
        h2o.init(1, java_heap_GB=4)

    @classmethod 
    def tearDownClass(cls): 
        h2o.tear_down_cloud()
 
    def test_overlap_diff_dest_stallN(self):
        noPoll = True
        num_trials = 0 
        stallForNJobs = 25
        for i in range(2):
            for j in range(30):
                csvFilename = 'poker-hand-testing.data'
                csvPathname = 'poker/' + csvFilename
                src_key = csvFilename + "_" + str(i) + "_" + str(j)
                hex_key =  csvFilename + "_" + str(num_trials) + '.hex'
                parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, schema='put',
                    src_key=src_key, hex_key=hex_key, timeoutSecs=120, noPoll=noPoll,
                    doSummary=False)
                num_trials += 1
            h2o_jobs.pollWaitJobs(timeoutSecs=300, pollTimeoutSecs=300, retryDelaySecs=5,stallForNJobs=stallForNJobs)

    def test_sequential_diff_dest(self):
        csvPathname = 'poker/poker-hand-testing.data'
        for trials in range(30):
            hex_key = csvPathname + "_" + str(trials)
            parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, schema='put',
                hex_key=hex_key, timeoutSecs=120, noPoll=False, doSummary=False)
        h2o_jobs.pollWaitJobs(timeoutSecs=300, pollTimeoutSecs=300, retryDelaySecs=5)

    def test_sequential_same_dest(self):
        csvPathname = 'poker/poker-hand-testing.data'
        for trials in range(30):
            src_key = csvPathname
            hex_key = csvPathname + '_' + str(trials) + '.hex'
            parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, schema='put',
                src_key=src_key, hex_key=hex_key, timeoutSecs=120, noPoll=False, doSummary=False)
        h2o_jobs.pollWaitJobs(timeoutSecs=300, pollTimeoutSecs=300, retryDelaySecs=5)
    
    def test_sequential_same_dest_del(self):
        csvFilename = 'poker-hand-testing.data'
        csvPathname = 'poker/' + csvFilename
        for trials in range(30):
            src_key = csvPathname
            hex_key = csvPathname + '.hex'
            parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, schema='put',
                src_key=src_key, hex_key=hex_key, timeoutSecs=120, noPoll=False, doSummary=False)
            h2o.nodes[0].remove_key(src_key)
            h2o.nodes[0].remove_key(hex_key)
        h2o_jobs.pollWaitJobs(timeoutSecs=300, pollTimeoutSecs=300, retryDelaySecs=5)

    @unittest.expectedFailure
    def test_overlap_same_dest_del_nopoll(self):
        csvPathname = 'poker/poker-hand-testing.data'
        for trials in range(30):
            src_key = csvPathname  
            hex_key = csvPathname + '.hex'
            parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, schema='put',
                src_key=src_key, hex_key=hex_key, timeoutSecs=120, noPoll=True, doSummary=False)
            h2o.nodes[0].remove_key(csvPathname)
        h2o_jobs.pollWaitJobs(timeoutSecs=300, pollTimeoutSecs=300, retryDelaySecs=5)

    @unittest.expectedFailure
    def test_overlap_same_dest_nopoll(self):
        for num_trials in range(30):
            csvPathname = 'poker/poker-hand-testing.data'
            src_key = csvPathname
            hex_key = csvPathname + '.hex'
            parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, schema='put',
                src_key=src_key, hex_key=hex_key, timeoutSecs=120, noPoll=True, doSummary=False)
        h2o_jobs.pollWaitJobs(timeoutSecs=300, pollTimeoutSecs=300, retryDelaySecs=5)

if __name__ == "__main__":
    h2o.unit_main()
