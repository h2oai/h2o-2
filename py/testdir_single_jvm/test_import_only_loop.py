import unittest, time, sys, random
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_glm, h2o_browse as h2b, h2o_import as h2i, h2o_exec as h2e

ITERATIONS = 20
DELETE_ON_DONE = 1
DO_EXEC = True
DO_UNCOMPRESSED = False
class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        # assume we're at 0xdata with it's hdfs namenode
        h2o.init(java_heap_GB=3)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_parse_manyfiles_1(self):
        # these will be used as directory imports/parse
        csvDirname = "manyfiles-nflx-gz"
        timeoutSecs = 600
        trial = 0
        for iteration in range(ITERATIONS):
            
            if DO_UNCOMPRESSED:
                csvFilename = "a_1.dat"
            else:
                csvFilename = "file_1.dat.gz"
            csvPathname = csvDirname + "/" + csvFilename
            trialStart = time.time()
            # import***************************************** 
            hex_key =  csvFilename + "_" + str(trial) + ".hex"
            start = time.time()
                
            # the import has to overwrite existing keys. no parse
            h2i.import_only(bucket='home-0xdiag-datasets', path=csvPathname, schema='put', hex_key=hex_key,
                timeoutSecs=timeoutSecs, retryDelaySecs=10, pollTimeoutSecs=120, doSummary=False)
            elapsed = time.time() - start
            print "import", trial, "end ", 'took', elapsed, 'seconds',\
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)

            # STOREVIEW***************************************
            print "\nTrying StoreView after the import"
            for node in h2o.nodes:
                h2o_cmd.runStoreView(node=node, timeoutSecs=30, view=10000)

            # exec does read lock on all existing keys
            if DO_EXEC:
                # fails
                execExpr="A.hex=c(0,1)"
                # execExpr="A.hex=0;"
                h2e.exec_expr(execExpr=execExpr, timeoutSecs=20)
                h2o_cmd.runInspect(key='A.hex')

            print "\nTrying StoreView after the exec "
            h2o_cmd.runStoreView(timeoutSecs=30, view=10000)
            # for node in h2o.nodes:
            #    h2o_cmd.runStoreView(node=node, timeoutSecs=30, view=10000)

            print "Trial #", trial, "completed in", time.time() - trialStart, "seconds."
            trial += 1

if __name__ == '__main__':
    h2o.unit_main()
