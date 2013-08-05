import unittest
import random, sys, time, os
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import as h2i, h2o_exec as h2e, h2o_glm

zeroList = [
        'Result0 = 0',
]

# the first column should use this
exprList = [
        'Result<n> = sum(<keyX>[<col1>])',
    ]

DO_SUMMARY=True

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED, localhost
        SEED = h2o.setup_random_seed()
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(3,java_heap_GB=4)
        else:
            h2o_hosts.build_cloud_with_hosts() # uses import Hdfs for s3n instead of import folder

    @classmethod
    def tearDownClass(cls):
        # wait while I inspect things
        # time.sleep(1500)
        h2o.tear_down_cloud()

    def test_parse_bounds_libsvm(self):
        # just do the import folder once
        importFolderPath = "/home/0xdiag/datasets/libsvm"

        # make the timeout variable per dataset. it can be 10 secs for covtype 20x (col key creation)
        # so probably 10x that for covtype200
        csvFilenameList = [
            # multi-label target like 1,2,5 ..not sure what that means
            # ("tmc2007_train.svm",  "cJ", 30, 0, 21.0),
            ("syn_6_1000_10.svm",  "cK", 30, -36, 36),
            ("syn_0_100_1000.svm", "cL", 30, -36, 36), 
            ("mnist_training.svm", "cM", 30, 0, 9),
            ("colon-cancer.svm",   "cA", 30, -1.000000, 1.000000),
            ("news20.svm",         "cH", 30, 1, 20), 
            ("connect4.svm",       "cB", 30, -1, 1),
            ("covtype.binary.svm", "cC", 30, 1, 2),
            ("duke.svm",           "cD", 30, -1.000000, 1.000000),
            # too many features? 150K inspect timeout?
            # ("E2006.train.svm",    "cE", 30, 1, -7.89957807346873 -0.519409526940154)

            ("gisette_scale.svm",  "cF", 30, -1, 1),
            ("mushrooms.svm",      "cG", 30, 1, 2),
        ]

        ### csvFilenameList = random.sample(csvFilenameAll,1)
        # h2b.browseTheCloud()
        lenNodes = len(h2o.nodes)

        firstDone = False
        for (csvFilename, key2, timeoutSecs, expectedCol0Min, expectedCol0Max) in csvFilenameList:
            # have to import each time, because h2o deletes source after parse
            h2i.setupImportFolder(None, importFolderPath)
            # creates csvFilename.hex from file in importFolder dir 
            parseKey = h2i.parseImportFolderFile(None, csvFilename, importFolderPath, 
                key2=key2, timeoutSecs=2000)
            print csvFilename, 'parse time:', parseKey['response']['time']
            print "Parse result['destination_key']:", parseKey['destination_key']

            # INSPECT******************************************
            start = time.time()
            inspect = h2o_cmd.runInspect(None, parseKey['destination_key'], timeoutSecs=360)
            print "Inspect:", parseKey['destination_key'], "took", time.time() - start, "seconds"
            h2o_cmd.infoFromInspect(inspect, csvFilename)
            # look at the min/max for the target col (0) and compare to expected for the dataset
            
            imin = inspect['cols'][0]['min']
            imax = inspect['cols'][0]['max']

            if expectedCol0Min:
                self.assertEqual(imin, expectedCol0Min,
                    msg='col %s min %s is not equal to expected min %s' % (0, imin, expectedCol0Min))
            if expectedCol0Max:
                self.assertEqual(imax, expectedCol0Max,
                    msg='col %s max %s is not equal to expected max %s' % (0, imax, expectedCol0Max))

            print "\nmin/max for col0:", imin, imax

            # SUMMARY****************************************
            # gives us some reporting on missing values, constant values, 
            # to see if we have x specified well
            # figures out everything from parseKey['destination_key']
            # needs y to avoid output column (which can be index or name)
            # assume all the configs have the same y..just check with the firs tone
            if DO_SUMMARY:
                goodX = h2o_glm.goodXFromColumnInfo(y=0,
                    key=parseKey['destination_key'], timeoutSecs=300, noPrint=True)
                summaryResult = h2o_cmd.runSummary(key=key2, timeoutSecs=360)
                h2o_cmd.infoFromSummary(summaryResult, noPrint=True)



if __name__ == '__main__':
    h2o.unit_main()
