import unittest
import random, sys, time, re
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i, h2o_glm, h2o_util, h2o_rf

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init(3, java_heap_GB=4)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_parse_mnist_rebalance(self):
        importFolderPath = "mnist"
        csvFilelist = [
            ("mnist_training.csv.gz", 600),
            ("mnist_training.csv.gz", 600),
            ("mnist_testing.csv.gz", 600),
            ("mnist_testing.csv.gz", 600),
        ]

        trial = 0
        allDelta = []
        for (csvFilename,  timeoutSecs) in csvFilelist:
            hex_key = csvFilename + "_" + str(trial) + ".hex"
            start = time.time()
            parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=importFolderPath+"/"+csvFilename,
                hex_key=hex_key, retryDelaySecs=1, timeoutSecs=timeoutSecs)
            elapsed = time.time() - start
            print "parse end on ", csvFilename, 'took', elapsed, 'seconds',\
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)

            print "\n#******************************************************************************" 
            for trial in range(1):
                rb_key = "rb_%s_%s" % (trial, hex_key)
                SEEDPERFILE = random.randint(0, sys.maxint)
                randChunks = random.randint(1, 100)
                start = time.time()
                print "Trial %s: Rebalancing %s to %s with %s chunks" % (trial, hex_key, rb_key, randChunks)
                rebalanceResult = h2o.nodes[0].rebalance(source=hex_key, after=rb_key, chunks=randChunks)
                elapsed = time.time() - start
                print "rebalance end on ", csvFilename, 'took', elapsed, 'seconds',\
                h2o_cmd.runSummary(key=rb_key, timeoutSecs=timeoutSecs)
                print "\nInspecting the original parsed result"
                inspect = h2o_cmd.runInspect(key=hex_key)
                h2o_cmd.infoFromInspect(inspect=inspect)
                print "\nInspecting the rebalanced result with %s forced chunks" % randChunks
                inspect = h2o_cmd.runInspect(key=rb_key)
                h2o_cmd.infoFromInspect(inspect=inspect)


if __name__ == '__main__':
    h2o.unit_main()
