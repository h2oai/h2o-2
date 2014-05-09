import unittest, random, sys, time, os
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
        importFolderPath = "libsvm"

        # make the timeout variable per dataset. it can be 10 secs for covtype 20x (col key creation)
        # so probably 10x that for covtype200
        csvFilenameList = [
            ("mnist_train.svm", "cM", 30, 0, 9.0, False, False),
            ("covtype.binary.svm", "cC", 30, 1, 2.0, True, True),
            # multi-label target like 1,2,5 ..not sure what that means
            # ("tmc2007_train.svm",  "cJ", 30, 0, 21.0, False, False),
            # illegal non-ascending cols
            # ("syn_6_1000_10.svm",  "cK", 30, -36, 36, True, False),
            # ("syn_0_100_1000.svm", "cL", 30, -36, 36, True, False), 
            # fails csvDownload
            ("duke.svm",           "cD", 30, -1.000000, 1.000000, False, False),
            ("colon-cancer.svm",   "cA", 30, -1.000000, 1.000000, False, False),
            ("news20.svm",         "cH", 30, 1, 20.0, False, False), 
            ("connect4.svm",       "cB", 30, -1, 1.0, False, False),
            # too many features? 150K inspect timeout?
            # ("E2006.train.svm",    "cE", 30, 1, -7.89957807346873 -0.519409526940154, False, False)

            ("gisette_scale.svm",  "cF", 30, -1, 1.0, False, False),
            ("mushrooms.svm",      "cG", 30, 1, 2.0, False, False),
        ]

        ### csvFilenameList = random.sample(csvFilenameAll,1)
        ### h2b.browseTheCloud()
        lenNodes = len(h2o.nodes)

        firstDone = False
        for (csvFilename, hex_key, timeoutSecs, expectedCol0Min, expectedCol0Max, enableDownloadReparse, enableSizeChecks) in csvFilenameList:
            # have to import each time, because h2o deletes source after parse
            csvPathname = importFolderPath + "/" + csvFilename
            parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, 
                hex_key=hex_key, timeoutSecs=2000)
            print csvPathname, 'parse time:', parseResult['response']['time']
            print "Parse result['destination_key']:", parseResult['destination_key']

            # INSPECT******************************************
            start = time.time()
            inspectFirst = h2o_cmd.runInspect(None, parseResult['destination_key'], timeoutSecs=360)
            print "Inspect:", parseResult['destination_key'], "took", time.time() - start, "seconds"
            h2o_cmd.infoFromInspect(inspectFirst, csvFilename)
            # look at the min/max for the target col (0) and compare to expected for the dataset
            
            imin = float(inspectFirst['cols'][0]['min'])
            # print h2o.dump_json(inspectFirst['cols'][0])
            imax = float(inspectFirst['cols'][0]['max'])

            if expectedCol0Min:
                self.assertEqual(imin, expectedCol0Min,
                    msg='col %s min %s is not equal to expected min %s' % (0, imin, expectedCol0Min))
            if expectedCol0Max:
                h2o_util.assertApproxEqual(imax, expectedCol0Max, tol=0.00000001,
                    msg='col %s max %s is not equal to expected max %s' % (0, imax, expectedCol0Max))

            print "\nmin/max for col0:", imin, imax

            # SUMMARY****************************************
            # gives us some reporting on missing values, constant values, 
            # to see if we have x specified well
            # figures out everything from parseResult['destination_key']
            # needs y to avoid output column (which can be index or name)
            # assume all the configs have the same y..just check with the firs tone
            if DO_SUMMARY:
                goodX = h2o_glm.goodXFromColumnInfo(y=0,
                    key=parseResult['destination_key'], timeoutSecs=300, noPrint=True)
                summaryResult = h2o_cmd.runSummary(key=hex_key, timeoutSecs=360)
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
                h2o.nodes[0].csv_download(src_key=hex_key, csvPathname=csvDownloadPathname)

                # remove the original parsed key. source was already removed by h2o
                # don't have to now. we use a new name for hex_keyB
                # h2o.nodes[0].remove_key(hex_key)
                start = time.time()
                hex_keyB = hex_key + "_B"
                parseResultB = h2o_cmd.parseResult = h2i.import_parse(path=csvDownloadPathname, schema='put', hex_key=hex_keyB)
                print csvDownloadPathname, "download/reparse (B) parse end. Original data from", \
                    csvFilename, 'took', time.time() - start, 'seconds'
                inspect = h2o_cmd.runInspect(key=hex_keyB)

                missingValuesListB = h2o_cmd.infoFromInspect(inspect, csvPathname)
                num_colsB = inspect['num_cols']
                num_rowsB = inspect['num_rows']
                row_sizeB = inspect['row_size']
                value_size_bytesB = inspect['value_size_bytes']

                df = h2o_util.JsonDiff(inspectFirst, inspect, with_values=True)
                print "df.difference:", h2o.dump_json(df.difference)

                for i,d in enumerate(df.difference):
                    # ignore mismatches in these
                    #  "variance"
                    #  "response.time"
                    #  "key"
                    if "variance" in d or "response.time" in d or "key" in d or "value_size_bytes" in d or "row_size" in d:
                        pass
                    else: 
                        raise Exception ("testing %s, found unexpected mismatch in df.difference[%d]: %s" % (csvPathname, i, d))

                if DO_SIZE_CHECKS and enableSizeChecks: 
                    # if we're allowed to do size checks. ccompare the full json response!
                    print "Comparing original inspect to the inspect after parsing the downloaded csv"
                    # vice_versa=True
                    
                    # ignore the variance diffs. reals mismatch when they're not?
                    filtered = [v for v in df.difference if not 'variance' in v]
                    self.assertLess(len(filtered), 3,
                        msg="Want < 3, not %d differences between the two rfView json responses. %s" % \
                            (len(filtered), h2o.dump_json(filtered)))

                    # this fails because h2o writes out zeroes as 0.0000* which gets loaded as fp even if col is all zeroes
                    # only in the case where the libsvm dataset specified vals = 0, which shouldn't happen
                    # make the check conditional based on the dataset
                    self.assertEqual(row_sizeA, row_sizeB,
                        "row_size mismatches after re-parse of downloadCsv result %d %d" % (row_sizeA, row_sizeB))
                    h2o_util.assertApproxEqual(value_size_bytesA, value_size_bytesB, tol=0.00000001,
                        msg="value_size_bytes mismatches after re-parse of downloadCsv result %d %d" % (value_size_bytesA, value_size_bytesB))

                print "missingValuesListA:", missingValuesListA
                print "missingValuesListB:", missingValuesListB
                self.assertEqual(missingValuesListA, missingValuesListB,
                    "missingValuesList mismatches after re-parse of downloadCsv result")
                self.assertEqual(num_colsA, num_colsB,
                    "num_cols mismatches after re-parse of downloadCsv result %d %d" % (num_colsA, num_colsB))
                self.assertEqual(num_rowsA, num_rowsB,
                    "num_rows mismatches after re-parse of downloadCsv result %d %d" % (num_rowsA, num_rowsB))

            ### h2b.browseJsonHistoryAsUrlLastMatch("Inspect")
            h2o.check_sandbox_for_errors()

if __name__ == '__main__':
    h2o.unit_main()
