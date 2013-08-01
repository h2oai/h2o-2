import unittest
import random, sys, time, os
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import as h2i, h2o_exec as h2e

zeroList = [
        'Result0 = 0',
]

# the first column should use this
exprList = [
        'Result<n> = sum(<keyX>[<col1>])',
    ]

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

    def test_parse_libsvm(self):
        # just do the import folder once
        importFolderPath = "/home/0xdiag/datasets/libsvm"

        # make the timeout variable per dataset. it can be 10 secs for covtype 20x (col key creation)
        # so probably 10x that for covtype200
        csvFilenameList = [
            ("colon-cancer.svm",   "cA", 30, 1),
            ("connect4.svm",       "cB", 30, 1),
            ("covtype.binary.svm", "cC", 30, 1),
            ("duke.svm",           "cD", 30, 1),
            ("E2006.train.svm",    "cE", 30, 1),
            ("gisette_scale.svm",  "cF", 30, 1),
            ("mushrooms.svm",      "cG", 30, 1),
            ("news20.svm",         "cH", 30, 1),
            ("sector.libsvm.svm",  "cI", 30, 1),
            ("tmc2007_train.svm",  "cJ", 30, 1),
            ("covtype.data",       "cK", 30,  1),
        ]

        ### csvFilenameList = random.sample(csvFilenameAll,1)
        # h2b.browseTheCloud()
        lenNodes = len(h2o.nodes)

        firstDone = False
        for (csvFilename, key2, timeoutSecs, resultMult) in csvFilenameList:
            # have to import each time, because h2o deletes source after parse
            h2i.setupImportFolder(None, importFolderPath)
            # creates csvFilename.hex from file in importFolder dir 
            parseKey = h2i.parseImportFolderFile(None, csvFilename, importFolderPath, 
                key2=key2, timeoutSecs=2000)
            print csvFilename, 'parse time:', parseKey['response']['time']
            print "Parse result['destination_key']:", parseKey['destination_key']

            # We should be able to see the parse result?
            inspect = h2o_cmd.runInspect(None, parseKey['destination_key'])

            print "\n" + csvFilename
            h2e.exec_zero_list(zeroList)
            colResultList = h2e.exec_expr_list_across_cols(lenNodes, exprList, key2, maxCol=54, 
                timeoutSecs=timeoutSecs)
            print "\n*************"
            print "colResultList", colResultList
            print "*************"

            if not firstDone:
                colResultList0 = list(colResultList)
                good = [float(x) for x in colResultList0] 
                firstDone = True
            else:
                print "\n", colResultList0, "\n", colResultList
                # create the expected answer...i.e. N * first
                compare = [float(x)/resultMult for x in colResultList] 
                print "\n", good, "\n", compare
                # self.assertEqual(good, compare, 'compare is not equal to good (first try * resultMult)')
        

if __name__ == '__main__':
    h2o.unit_main()
