import unittest
import random, sys, time, os
sys.path.extend(['.','..','../..','py'])

import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init(2,java_heap_GB=6)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_rf_libsvm_fvec(self):
        # just do the import folder once

        # make the timeout variable per dataset. it can be 10 secs for covtype 20x (col key creation)
        # so probably 10x that for covtype200
        csvFilenameList = [
            ("mnist_train.svm", "cM", 30, 1, 1),
            ("covtype.binary.svm", "cC", 30, 1, 1),
            ("gisette_scale.svm",  "cF", 30, 1, 0),
            # FIX! fails KMeansScore
            # not integer output
            # ("colon-cancer.svm",   "cA", 30, 1, 1),
            ("connect4.svm",       "cB", 30, 1, 1),
            # ("syn_6_1000_10.svm",  "cK", 30, 1, 0),   Bad libsvm file has the same column multiple times.
            # float response requires regression
            ("syn_0_100_1000.svm", "cL", 30, 1, 0),
            ("mushrooms.svm",      "cG", 30, 1, 1),
            # rf doesn't like reals
            # ("duke.svm",           "cD", 30, 1, 1),
            # too many features? 150K inspect timeout?
            # ("E2006.train.svm",    "cE", 30, 1, 1),
            # too big for rf (memory error)
            # ("news20.svm",         "cH", 30, 1, 1),

            # multiclass format ..don't support
            # ("tmc2007_train.svm",  "cJ", 30, 1, 1),
            # normal csv
        ]

        ### csvFilenameList = random.sample(csvFilenameAll,1)
        # h2b.browseTheCloud()
        lenNodes = len(h2o.nodes)

        firstDone = False
        for (csvFilename, hex_key, timeoutSecs, resultMult, classification) in csvFilenameList:
            # have to import each time, because h2o deletes source after parse
            bucket = "home-0xdiag-datasets"
            csvPathname = "libsvm/" + csvFilename

            # PARSE******************************************
            parseResult = h2i.import_parse(bucket=bucket, path=csvPathname, schema='put', hex_key=hex_key, timeoutSecs=2000)
            print "Parse result['destination_key']:", parseResult['destination_key']

            # INSPECT******************************************
            start = time.time()
            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'], timeoutSecs=360)
            print "Inspect:", parseResult['destination_key'], "took", time.time() - start, "seconds"
            h2o_cmd.infoFromInspect(inspect, csvFilename)

            # RF******************************************
            kwargs = {
                'ntrees': 1,
                'response': 0,
                'classification': classification,
                'importance': 0,
            }

            timeoutSecs = 600
            start = time.time()
            rf = h2o_cmd.runRF(parseResult=parseResult, timeoutSecs=timeoutSecs, **kwargs)
            elapsed = time.time() - start
            print "rf end on ", csvPathname, 'took', elapsed, 'seconds.', \
                "%d pct. of timeout" % ((elapsed/timeoutSecs) * 100)


if __name__ == '__main__':
    h2o.unit_main()
