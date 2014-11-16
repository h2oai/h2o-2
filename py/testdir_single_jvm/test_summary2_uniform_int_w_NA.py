import unittest, time, sys, random, math, getpass
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_import as h2i, h2o_util, h2o_print as h2p
import h2o_summ, h2o_browse as h2b, h2o_util

print "Like test_summary_uniform, but with integers only"

DO_MEDIAN = False
MAX_QBINS = 1
MAX_QBINS = 1000000
DO_REAL = False

ROWS = 10000 # passes
ROWS = 100000 # passes
ROWS = 1000000 # corrupted hcnt2_min/max and ratio 5
NA_ROW_RATIO = 1

def write_syn_dataset(csvPathname, rowCount, colCount, expectedMin, expectedMax, SEED):
    r1 = random.Random(SEED)
    dsf = open(csvPathname, "w+")

    expectedRange = (expectedMax - expectedMin)
    for i in range(rowCount):
        rowData = []
        if DO_REAL:
            ri = expectedMin + (random.random() * expectedRange)
        else:
            ri = random.randint(expectedMin,expectedMax)
        for j in range(colCount):
            rowData.append(ri)
        rowDataCsv = ",".join(map(str,rowData))
        for k in range(NA_ROW_RATIO):
          dsf.write(rowDataCsv + "\n")

    dsf.close()

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init()
        # h2b.browseTheCloud()

    @classmethod
    def tearDownClass(cls):
        # h2o.sleep(3600)
        h2o.tear_down_cloud()

    def test_summary2_uniform_int_w_NA(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        M = 100
        tryList = [
            # colname, (min, 25th, 50th, 75th, max)
            (ROWS, 1, 'B.hex', 1, 1000*M,            ('C1',  1.0*M, 250.0*M, 500.0*M, 750.0*M, 1000.0*M)),
            (ROWS, 1, 'B.hex', 1, 1000,            ('C1',  1.0, 250.0, 500.0, 750.0, 1000.0)),
            (ROWS, 1, 'x.hex', 1, 20000,           ('C1',  1.0, 5000.0, 10000.0, 15000.0, 20000.0)),
            (ROWS, 1, 'x.hex', -5000, 0,           ('C1', -5000.00, -3750.0, -2500.0, -1250.0, 0)),
            (ROWS, 1, 'x.hex', -100000, 100000,    ('C1',  -100000.0, -50000.0, 0, 50000.0, 100000.0)),

            # (ROWS, 1, 'A.hex', 1, 101,             ('C1',   1.0, 26.00, 51.00, 76.00, 101.0)),
            # (ROWS, 1, 'A.hex', -99, 99,            ('C1',  -99, -49.0, 0, 49.00, 99)),

            (ROWS, 1, 'B.hex', 1, 10000,           ('C1',   1.0, 2501.0, 5001.0, 7501.0, 10000.0)),
            (ROWS, 1, 'B.hex', -100, 100,          ('C1',  -100.0, -50.0, 0.0, 50.0, 100.0)),

            (ROWS, 1, 'C.hex', 1, 100000,          ('C1',   1.0, 25001.0, 50001.0, 75001.0, 100000.0)),
            # (ROWS, 1, 'C.hex', -101, 101,          ('C1',  -101, -51, -1, 49.0, 100.0)),
        ]
        if not DO_REAL:
            # only 3 integer values!
            tryList.append(\
                (1000000, 1, 'x.hex', -1, 1,              ('C1',  -1.0, -1, 0.000, 1, 1.00)) \
                )

        timeoutSecs = 10
        trial = 1
        n = h2o.nodes[0]
        lenNodes = len(h2o.nodes)

        x = 0
        timeoutSecs = 60
        for (rowCount, colCount, hex_key, expectedMin, expectedMax, expected) in tryList:
            # max error = half the bin size?
        
            maxDelta = ((expectedMax - expectedMin)/(MAX_QBINS + 0.0)) 
            # add 5% for fp errors?
            maxDelta = 1.05 * maxDelta
            # also need to add some variance due to random distribution?
            # maybe a percentage of the mean
            distMean = (expectedMax - expectedMin) / 2
            maxShift = distMean * .01
            maxDelta = maxDelta + maxShift

            SEEDPERFILE = random.randint(0, sys.maxint)
            x += 1

            csvFilename = 'syn_' + "binary" + "_" + str(rowCount) + 'x' + str(colCount) + '.csv'
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename

            print "Creating random", csvPathname
            write_syn_dataset(csvPathname, rowCount, colCount, expectedMin, expectedMax, SEEDPERFILE)
            csvPathnameFull = h2i.find_folder_and_filename(None, csvPathname, returnFullPath=True)
            parseResult = h2i.import_parse(path=csvPathname, schema='put', hex_key=hex_key, timeoutSecs=60, doSummary=False)
            print "Parse result['destination_key']:", parseResult['destination_key']

            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
            print "\n" + csvFilename

            numRows = inspect["numRows"]
            numCols = inspect["numCols"]

            summaryResult = h2o_cmd.runSummary(key=hex_key, max_qbins=MAX_QBINS)
            h2o.verboseprint("summaryResult:", h2o.dump_json(summaryResult))

            # only one column
            column = summaryResult['summaries'][0]
            colname = column['colname']
            self.assertEqual(colname, expected[0])

            coltype = column['type']
            nacnt = column['nacnt']

            stats = column['stats']
            stattype= stats['type']

            # FIX! we should compare mean and sd to expected?
            mean = stats['mean']
            sd = stats['sd']

            print "colname:", colname, "mean (2 places):", h2o_util.twoDecimals(mean)
            print "colname:", colname, "std dev. (2 places):", h2o_util.twoDecimals(sd)

            zeros = stats['zeros']
            mins = stats['mins']
            maxs = stats['maxs']
            h2o_util.assertApproxEqual(mins[0], expected[1], tol=maxDelta, msg='min is not approx. expected')
            h2o_util.assertApproxEqual(maxs[0], expected[5], tol=maxDelta, msg='max is not approx. expected')

            pct = stats['pct']
            # the thresholds h2o used, should match what we expected
            expectedPct= [0.01, 0.05, 0.1, 0.25, 0.33, 0.5, 0.66, 0.75, 0.9, 0.95, 0.99]

            pctile = stats['pctile']
            h2o_util.assertApproxEqual(pctile[3], expected[2], tol=maxDelta, msg='25th percentile is not approx. expected')
            h2o_util.assertApproxEqual(pctile[5], expected[3], tol=maxDelta, msg='50th percentile (median) is not approx. expected')
            h2o_util.assertApproxEqual(pctile[7], expected[4], tol=maxDelta, msg='75th percentile is not approx. expected')

            hstart = column['hstart']
            hstep = column['hstep']
            hbrk = column['hbrk']
            hcnt = column['hcnt']

            print "pct:", pct
            print "hcnt:", hcnt
            print "len(hcnt)", len(hcnt)

            # don't check the last bin
            for b in hcnt[1:-1]:
                # should we be able to check for a uniform distribution in the files?
                e = numRows/len(hcnt) # expect 21 thresholds, so 20 bins. each 5% of rows (uniform distribution)
                # don't check the edge bins
                self.assertAlmostEqual(b, rowCount/len(hcnt), delta=.01*rowCount, 
                    msg="Bins not right. b: %s e: %s" % (b, e))

            pt = h2o_util.twoDecimals(pctile)
            mx = h2o_util.twoDecimals(maxs)
            mn = h2o_util.twoDecimals(mins)
            print "colname:", colname, "pctile (2 places):", pt
            print "colname:", colname, "maxs: (2 places):", mx
            print "colname:", colname, "mins: (2 places):", mn

            # FIX! we should do an exec and compare using the exec quantile too
            compareActual = mn[0], pt[3], pt[5], pt[7], mx[0]
            h2p.green_print("min/25/50/75/max colname:", colname, "(2 places):", compareActual)
            print "maxs colname:", colname, "(2 places):", mx
            print "mins colname:", colname, "(2 places):", mn

            trial += 1

            scipyCol = 0 
            # don't check if colname is empty..means it's a string and scipy doesn't parse right?
            if colname!='':
                # don't do for enums
                # also get the median with a sort (h2o_summ.percentileOnSortedlist()
                h2o_summ.quantile_comparisons(
                    csvPathnameFull,
                    col=0, # what col to extract from the csv
                    datatype='float',
                    quantile=0.5 if DO_MEDIAN else 0.999,
                    h2oSummary2=pctile[5 if DO_MEDIAN else 10],
                    # h2oQuantilesApprox=qresult_single,
                    # h2oQuantilesExact=qresult,
                    )

            h2o.nodes[0].remove_all_keys()



if __name__ == '__main__':
    h2o.unit_main()

