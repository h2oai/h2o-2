import unittest
import random, sys, time, os
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import as h2i, h2o_exec as h2e, h2o_glm, h2o_util

zeroList = [
        'Result0 = 0',
]

# the first column should use this
exprList = [
        'Result<n> = sum(<keyX>[<col1>])',
    ]

DO_SUMMARY=True
DO_DOWNLOAD_REPARSE=True
DO_SIZE_CHECKS=True

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
        SYNDATASETS_DIR = h2o.make_syn_dir()

        # just do the import folder once
        importFolderPath = "/home/0xdiag/datasets/libsvm"

        # make the timeout variable per dataset. it can be 10 secs for covtype 20x (col key creation)
        # so probably 10x that for covtype200
        csvFilenameList = [
            ("covtype.binary.svm", "cC", 30, 1, 2, True, True),
            ("mnist_train.svm", "cM", 30, 0, 9, False, False),
            # multi-label target like 1,2,5 ..not sure what that means
            # ("tmc2007_train.svm",  "cJ", 30, 0, 21.0, False, False),
            ("syn_6_1000_10.svm",  "cK", 30, -36, 36, True, False),
            ("syn_0_100_1000.svm", "cL", 30, -36, 36, True, False), 
            # fails csvDownload
            ("duke.svm",           "cD", 30, -1.000000, 1.000000, False, False),
            ("colon-cancer.svm",   "cA", 30, -1.000000, 1.000000, False, False),
            ("news20.svm",         "cH", 30, 1, 20, False, False), 
            ("connect4.svm",       "cB", 30, -1, 1, False, False),
            # too many features? 150K inspect timeout?
            # ("E2006.train.svm",    "cE", 30, 1, -7.89957807346873 -0.519409526940154, False, False)

            ("gisette_scale.svm",  "cF", 30, -1, 1, False, False),
            ("mushrooms.svm",      "cG", 30, 1, 2, False, False),
        ]

        ### csvFilenameList = random.sample(csvFilenameAll,1)
        h2b.browseTheCloud()
        lenNodes = len(h2o.nodes)

        firstDone = False
        for (csvFilename, key2, timeoutSecs, expectedCol0Min, expectedCol0Max, enableDownloadReparse, enableSizeChecks) in csvFilenameList:
            # have to import each time, because h2o deletes source after parse
            h2i.setupImportFolder(None, importFolderPath)
            csvPathname = importFolderPath + "/" + csvFilename
            # creates csvFilename.hex from file in importFolder dir 
            parseKey = h2i.parseImportFolderFile(None, csvFilename, importFolderPath, 
                key2=key2, timeoutSecs=2000)
            print csvPathname, 'parse time:', parseKey['response']['time']
            print "Parse result['destination_key']:", parseKey['destination_key']

            # INSPECT******************************************
            start = time.time()
            inspectFirst = h2o_cmd.runInspect(None, parseKey['destination_key'], timeoutSecs=360)
            print "Inspect:", parseKey['destination_key'], "took", time.time() - start, "seconds"
            h2o_cmd.infoFromInspect(inspectFirst, csvFilename)
            # look at the min/max for the target col (0) and compare to expected for the dataset
            
            imin = inspectFirst['cols'][0]['min']
            imax = inspectFirst['cols'][0]['max']

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

            if DO_DOWNLOAD_REPARSE and enableDownloadReparse:
                missingValuesListA = h2o_cmd.infoFromInspect(inspectFirst, csvPathname)
                num_colsA = inspectFirst['num_cols']
                num_rowsA = inspectFirst['num_rows']
                row_sizeA = inspectFirst['row_size']
                value_size_bytesA = inspectFirst['value_size_bytes']

                # do a little testing of saving the key as a csv
                csvDownloadPathname = SYNDATASETS_DIR + "/" + csvFilename + "_csvDownload.csv"
                print "Trying csvDownload of", csvDownloadPathname
                h2o.nodes[0].csv_download(key=key2, csvPathname=csvDownloadPathname)

                # remove the original parsed key. source was already removed by h2o
                # don't have to now. we use a new name for key2B
                # h2o.nodes[0].remove_key(key2)
                start = time.time()
                key2B = key2 + "_B"
                parseKeyB = h2o_cmd.parseFile(csvPathname=csvDownloadPathname, key2=key2B)
                print csvDownloadPathname, "download/reparse (B) parse end. Original data from", \
                    csvFilename, 'took', time.time() - start, 'seconds'
                inspect = h2o_cmd.runInspect(key=key2B)

                missingValuesListB = h2o_cmd.infoFromInspect(inspect, csvPathname)
                num_colsB = inspect['num_cols']
                num_rowsB = inspect['num_rows']
                row_sizeB = inspect['row_size']
                value_size_bytesB = inspect['value_size_bytes']

                self.assertEqual(missingValuesListA, missingValuesListB,
                    "missingValuesList mismatches after re-parse of downloadCsv result")
                self.assertEqual(num_colsA, num_colsB,
                    "num_cols mismatches after re-parse of downloadCsv result %d %d" % (num_colsA, num_colsB))
                self.assertEqual(num_rowsA, num_rowsB,
                    "num_rows mismatches after re-parse of downloadCsv result %d %d" % (num_rowsA, num_rowsB))
                if DO_SIZE_CHECKS and enableSizeChecks: 
                    # if we're allowed to do size checks. ccompare the full json response!
                    print "Comparing original inspect to the inspect after parsing the downloaded csv"
                    # vice_versa=True
                    df = h2o_util.JsonDiff(inspectFirst, inspect, with_values=True)
                    print "df.difference:", h2o.dump_json(df.difference)
                    self.assertGreater(len(df.difference), 29,
                        msg="Want >=30 , not %d differences between the two rfView json responses. %s" % \
                            (len(df.difference), h2o.dump_json(df.difference)))

                    # this fails because h2o writes out zeroes as 0.0000* which gets loaded as fp even if col is all zeroes
                    # only in the case where the libsvm dataset specified vals = 0, which shouldn't happen
                    # make the check conditional based on the dataset
                    self.assertEqual(row_sizeA, row_sizeB,
                        "row_size mismatches after re-parse of downloadCsv result %d %d" % (row_sizeA, row_sizeB))
                    self.assertEqual(value_size_bytesA, value_size_bytesB,
                        "value_size_bytes mismatches after re-parse of downloadCsv result %d %d" % (value_size_bytesA, value_size_bytesB))

            ### h2b.browseJsonHistoryAsUrlLastMatch("Inspect")
            h2o.check_sandbox_for_errors()



if __name__ == '__main__':
    h2o.unit_main()
