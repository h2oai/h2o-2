import unittest, time, sys, random, math, getpass
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_import as h2i, h2o_util, h2o_browse as h2b, h2o_print as h2p
import h2o_summ

print "same as test_summary2_unifiles.py but using local runif_.csv single col for comparison testing"

print "\nShould really add something that sees we go to 16 with no answer, if bins are set to 1"
print "Answer not guaranteed (for any data) if max iterations is 16 in h2o and max_qbins is small"

print "FIX! should loop across all the quantiles in summary2, and do them for all, for the datasets here"
print "0.99 had failed with the breadth.csv"
# the old failure
# Exception: h2o quantile multipass is not approx. same as sort algo. 
# h2o_util.assertApproxEqual failed comparing 21.0 and 24.84. {'tol': 2e-07}.

DO_MEDIAN = False
# 
# the test compares to summary which has fixed quantiles.
# but if we DO_MEDIAN=False, we can pick from any "other" here
thresholds = [0.001, 0.01, 0.1, 0.25, 0.33, 0.5, 0.66, 0.75, 0.9, 0.99, 0.999]
OTHER_Q_SUMM_INDEX = 3
# has to point to one of the summmary2 choices
OTHER_Q = thresholds[OTHER_Q_SUMM_INDEX]

MAX_QBINS = 49

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_summary2_unifiles2(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        # new with 1000 bins. copy expected from R
        tryList = [
            # colname, (min, 25th, 50th, 75th, max)
            ('breadth.csv', 'b.hex', False, [ ('C1', None, None, None, None, None)], 'smalldata', 'quantiles'),
            # ('wonkysummary.csv', 'b.hex', False, [ ('X1', 7, 22, 876713, 100008, 1000046)], 'smalldata', None),
            ('wonkysummary.csv', 'b.hex', True, [ ('X1', 7.00, None, None, None, 1000046.0)], 'smalldata', None),
            ('covtype.data', 'c.hex', False, [ ('C1', None, None, None, None, None)], 'home-0xdiag-datasets', 'standard'),

        ]

        timeoutSecs = 10
        trial = 1
        n = h2o.nodes[0]
        lenNodes = len(h2o.nodes)

        x = 0
        timeoutSecs = 60
        for (csvFilename, hex_key, skipHeader, expectedCols, bucket, pathPrefix) in tryList:

            if pathPrefix:
                csvPathname = pathPrefix + "/" + csvFilename
            else:
                csvPathname = csvFilename

            csvPathnameFull = h2i.find_folder_and_filename(bucket, csvPathname, returnFullPath=True)
            if skipHeader:
                header = 1
            else:
                header = 0
            parseResult = h2i.import_parse(bucket=bucket, path=csvPathname, 
                schema='put', header=header, hex_key=hex_key, timeoutSecs=10, doSummary=False)

            print "Parse result['destination_key']:", parseResult['destination_key']

            # We should be able to see the parse result?
            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
            print "\n" + csvFilename

            numRows = inspect["numRows"]
            numCols = inspect["numCols"]

            # okay to get more cols than we want
            summaryResult = h2o_cmd.runSummary(key=hex_key, max_qbins=MAX_QBINS)
            h2o.verboseprint("summaryResult:", h2o.dump_json(summaryResult))

            summaries = summaryResult['summaries']

            scipyCol = 0
            for expected, column in zip(expectedCols, summaries):
                colname = column['colname']
                if expected[0]:
                    self.assertEqual(colname, expected[0])

                quantile = 0.5 if DO_MEDIAN else OTHER_Q
                q = h2o.nodes[0].quantiles(source_key=hex_key, column=scipyCol,
                    quantile=quantile, max_qbins=MAX_QBINS, multiple_pass=2)
                qresult = q['result']
                qresult_single = q['result_single']
                qresult_iterations = q['iterations']
                qresult_interpolated = q['interpolated']
                h2p.blue_print("h2o quantiles result:", qresult)
                h2p.blue_print("h2o quantiles result_single:", qresult_single)
                h2p.blue_print("h2o quantiles iterations:", qresult_iterations)
                h2p.blue_print("h2o quantiles interpolated:", qresult_interpolated)
                print h2o.dump_json(q)

                self.assertLess(qresult_iterations, 16, 
                    msg="h2o does max of 16 iterations. likely no result_single if we hit max. is bins=1?")

                # ('',  '1.00', '25002.00', '50002.00', '75002.00', '100000.00'),
                coltype = column['type']
                nacnt = column['nacnt']

                stats = column['stats']
                stattype= stats['type']
                print stattype

                # FIX! we should compare mean and sd to expected?
                # enums don't have mean or sd?
                if stattype!='Enum':
                    mean = stats['mean']
                    sd = stats['sd']
                    zeros = stats['zeros']
                    mins = stats['mins']
                    maxs = stats['maxs']

                    print "colname:", colname, "mean (2 places):", h2o_util.twoDecimals(mean)
                    print "colname:", colname, "std dev. (2 places):", h2o_util.twoDecimals(sd)

                    pct = stats['pct']
                    print "pct:", pct
                    print ""

                    # the thresholds h2o used, should match what we expected
                    pctile = stats['pctile']

                # hack..assume just one None is enough to ignore for cars.csv
                if expected[1]:
                    h2o_util.assertApproxEqual(mins[0], expected[1], rel=0.02, msg='min is not approx. expected')
                if expected[2]:
                    h2o_util.assertApproxEqual(pctile[3], expected[2], rel=0.02, msg='25th percentile is not approx. expected')
                if expected[3]:
                    h2o_util.assertApproxEqual(pctile[5], expected[3], rel=0.02, msg='50th percentile (median) is not approx. expected')
                if expected[4]:
                    h2o_util.assertApproxEqual(pctile[7], expected[4], rel=0.02, msg='75th percentile is not approx. expected')
                if expected[5]:
                    h2o_util.assertApproxEqual(maxs[0], expected[5], rel=0.02, msg='max is not approx. expected')


                # figure out the expected max error
                # use this for comparing to sklearn/sort
                if expected[1] and expected[5]:
                    expectedRange = expected[5] - expected[1]
                    # because of floor and ceil effects due we potentially lose 2 bins (worst case)
                    # the extra bin for the max value, is an extra bin..ignore
                    expectedBin = expectedRange/(MAX_QBINS-2)
                    maxErr = 0.5 * expectedBin # should we have some fuzz for fp?
                    # hack?
                    maxErr = maxErr * 2
                    print "maxErr:", maxErr

                else:
                    print "Test won't calculate max expected error"
                    maxErr = 0

                hstart = column['hstart']
                hstep = column['hstep']
                hbrk = column['hbrk']
                hcnt = column['hcnt']

                for b in hcnt:
                    # should we be able to check for a uniform distribution in the files?
                    e = .1 * numRows
                    # self.assertAlmostEqual(b, .1 * rowCount, delta=.01*rowCount,
                    #     msg="Bins not right. b: %s e: %s" % (b, e))

                if stattype!='Enum':
                    pt = h2o_util.twoDecimals(pctile)
                    print "colname:", colname, "pctile (2 places):", pt
                    mx = h2o_util.twoDecimals(maxs)
                    mn = h2o_util.twoDecimals(mins)
                    print "colname:", colname, "maxs: (2 places):", mx
                    print "colname:", colname, "mins: (2 places):", mn

                    # FIX! we should do an exec and compare using the exec quantile too
                    actual = mn[0], pt[3], pt[5], pt[7], mx[0]
                    print "min/25/50/75/max colname:", colname, "(2 places):", actual
                    print "maxs colname:", colname, "(2 places):", mx
                    print "mins colname:", colname, "(2 places):", mn

                    ## ignore for blank colnames, issues with quoted numbers
                    # covtype is too big to do in scipy
                    if colname!='' and expected[scipyCol] and csvFilename!= 'covtype.data':
                        # don't do for enums
                        # also get the median with a sort (h2o_summ.percentileOnSortedlist()
                        h2o_summ.quantile_comparisons(
                            csvPathnameFull,
                            skipHeader=skipHeader, # important!!
                            col=scipyCol,
                            datatype='float',
                            quantile=0.5 if DO_MEDIAN else OTHER_Q,
                            h2oSummary2=pctile[5 if DO_MEDIAN else OTHER_Q_SUMM_INDEX],
                            h2oQuantilesApprox=qresult_single,
                            h2oQuantilesExact=qresult,
                            h2oSummary2MaxErr=maxErr,
                            )

                scipyCol += 1

            trial += 1



if __name__ == '__main__':
    h2o.unit_main()

