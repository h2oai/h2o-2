import unittest
import random, sys, time, os
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i, h2o_kmeans
class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init(2,java_heap_GB=7)

    @classmethod
    def tearDownClass(cls):
        # wait while I inspect things
        # h2o.sleep(1500)
        h2o.tear_down_cloud()

    def test_KMeans_libsvm_fvec(self):

        # hack this into a function so we can call it before and after kmeans
        # kmeans is changing the last col to enum?? (and changing the data)
        def do_summary_and_inspect():
            # SUMMARY******************************************
            summaryResult = h2o_cmd.runSummary(key=hex_key)
            coltypeList = h2o_cmd.infoFromSummary(summaryResult)

            # INSPECT******************************************
            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'], timeoutSecs=360)
            h2o_cmd.infoFromInspect(inspect, csvFilename)

            numRows = inspect['numRows']
            numCols = inspect['numCols']

            # Now check both inspect and summary
            if csvFilename=='covtype.binary.svm':
                for k in range(55):
                    naCnt = inspect['cols'][k]['naCnt']
                    self.assertEqual(0, naCnt, msg='col %s naCnt %d should be %s' % (k, naCnt, 0))
                    stype = inspect['cols'][k]['type']
                    print k, stype
                    self.assertEqual('Int', stype, msg='col %s type %s should be %s' % (k, stype, 'Int'))

                # summary may report type differently than inspect..check it too!
                # we could check na here too
                for i,c in enumerate(coltypeList):
                    print "column index: %s  column type: %s" % (i, c)
                    # inspect says 'int?"
                    assert c=='Numeric', "All cols in covtype.binary.svm should be parsed as Numeric! %s %s" % (i,c)

        # just do the import folder once
        # make the timeout variable per dataset. it can be 10 secs for covtype 20x (col key creation)
        # so probably 10x that for covtype200
        csvFilenameList = [
            # FIX! fails KMeansScore
            ("colon-cancer.svm",   "cA", 30, 1),
            ("connect4.svm",       "cB", 30, 1),
            ("covtype.binary.svm", "cC", 30, 1),
            # multi-label class
            # ("tmc2007_train.svm",  "cJ", 30, 1),
            ("mnist_train.svm", "cM", 30, 1),
            ("duke.svm",           "cD", 30, 1),
            # too many features? 150K inspect timeout?
            # ("E2006.train.svm",    "cE", 30, 1),
            ("gisette_scale.svm",  "cF", 120, 1), #Summary2 is slow with 5001 columns
            ("mushrooms.svm",      "cG", 30, 1),
    #        ("news20.svm",         "cH", 120, 1), #Summary2 is very slow - disable for now

            ("syn_6_1000_10.svm",  "cK", 30, 1),
            ("syn_0_100_1000.svm", "cL", 30, 1),
        ]

        csvFilenameList = [
            ("covtype.binary.svm", "cC", 30, 1),
        ]

        ### csvFilenameList = random.sample(csvFilenameAll,1)
        h2b.browseTheCloud()
        lenNodes = len(h2o.nodes)

        firstDone = False
        importFolderPath = "libsvm"
        for (csvFilename, hex_key, timeoutSecs, resultMult) in csvFilenameList:
            # have to import each time, because h2o deletes source after parse
            csvPathname = importFolderPath + "/" + csvFilename

            # PARSE******************************************
            # creates csvFilename.hex from file in importFolder dir 
            parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, 
                hex_key=hex_key, timeoutSecs=2000, doSummary=False)

            do_summary_and_inspect()

            # KMEANS******************************************
            for trial in range(1):
                kwargs = {
                    'k': 3, 
                    'initialization': 'Furthest',
                    'ignored_cols': None, #range(11, numCols), # THIS BREAKS THE REST API
                    'max_iter': 10,
                    # 'normalize': 0,
                    # reuse the same seed, to get deterministic results (otherwise sometimes fails
                    'seed': 265211114317615310,
                }

                # fails if I put this in kwargs..i.e. source = dest
                # 'destination_key': parseResult['destination_key'],

                timeoutSecs = 600
                start = time.time()
                kmeans = h2o_cmd.runKMeans(parseResult=parseResult, timeoutSecs=timeoutSecs, **kwargs)
                elapsed = time.time() - start
                print "kmeans end on ", csvPathname, 'took', elapsed, 'seconds.', \
                    "%d pct. of timeout" % ((elapsed/timeoutSecs) * 100)

                do_summary_and_inspect()

                # this does an inspect of the model and prints the clusters
                h2o_kmeans.simpleCheckKMeans(self, kmeans, **kwargs)

                print "hello"
                (centers, tupleResultList) = h2o_kmeans.bigCheckResults(self, kmeans, csvPathname, parseResult, 'd', **kwargs)

                do_summary_and_inspect()

if __name__ == '__main__':
    h2o.unit_main()
