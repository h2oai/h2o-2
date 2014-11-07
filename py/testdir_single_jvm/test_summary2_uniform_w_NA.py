import unittest, time, sys, random, math, getpass
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_import as h2i, h2o_util, h2o_print as h2p

if getpass.getuser()=='kevin' or getpass.getuser()=='jenkins':
    DO_TRY_SCIPY = True
# can't handle the NA in scipy?
DO_TRY_SCIPY = False

DO_MEDIAN = True
MAX_QBINS = 1000

# fails with 1M and NA
ROWS = 10000 # passes
ROWS = 100000 # passes
ROWS = 1000000 # corrupted hcnt2_min/max and ratio 5
NA_ROW_RATIO = 1

print "Same as test_summary2_uniform.py except for every data row,"
print "5 rows of synthetic NA rows are added. results should be the same for quantiles"

def write_syn_dataset(csvPathname, rowCount, colCount, expectedMin, expectedMax, SEED):
    r1 = random.Random(SEED)
    dsf = open(csvPathname, "w+")

    expectedRange = (expectedMax - expectedMin)
    for i in range(rowCount):
        rowData = []
        ri = expectedMin + (random.uniform(0,1) * expectedRange)
        for j in range(colCount):
            rowData.append(ri)
        rowDataCsv = ",".join(map(str,rowData))
        dsf.write(rowDataCsv + "\n")

        # add 5 rows of NAs, for every row of valid dat
        if 1==1:
            rowData = []
            for j in range(colCount):
                # this shouldn't be h2o parsed as an enum?...NA is special?
                rowData.append(',,')
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

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_summary2_uniform_w_NA(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        tryList = [
            # colname, (min, 25th, 50th, 75th, max)
            (ROWS, 1, 'x.hex', 1, 20000,        ('C1',  1.10, 5000.0, 10000.0, 15000.0, 20000.00)),
            (ROWS, 1, 'x.hex', -5000, 0,        ('C1', -5001.00, -3750.0, -2445, -1200.0, 99)),
            (ROWS, 1, 'x.hex', -100000, 100000, ('C1',  -100001.0, -50000.0, 1613.0, 50000.0, 100000.0)),
            (ROWS, 1, 'x.hex', -1, 1,           ('C1',  -1.05, -0.48, 0.0087, 0.50, 1.00)),

            (ROWS, 1, 'A.hex', 1, 100,          ('C1',   1.05, 26.00, 51.00, 76.00, 100.0)),
            (ROWS, 1, 'A.hex', -99, 99,         ('C1',  -99, -50.0, 0, 50.00, 99)),

            (ROWS, 1, 'B.hex', 1, 10000,        ('C1',   1.05, 2501.00, 5001.00, 7501.00, 10000.00)),
            (ROWS, 1, 'B.hex', -100, 100,       ('C1',  -100.10, -50.0, 0.85, 51.7, 100,00)),

            (ROWS, 1, 'C.hex', 1, 100000,       ('C1',   1.05, 25002.00, 50002.00, 75002.00, 100000.00)),
            (ROWS, 1, 'C.hex', -101, 101,       ('C1',  -100.10, -50.45, -1.18, 49.28, 100.00)),
        ]

        timeoutSecs = 10
        trial = 1
        n = h2o.nodes[0]
        lenNodes = len(h2o.nodes)

        x = 0
        timeoutSecs = 60
        for (rowCount, colCount, hex_key, expectedMin, expectedMax, expected) in tryList:
            # max error = half the bin size?
        
            maxDelta = ((expectedMax - expectedMin)/20.0) / 2.0
            # add 5% for fp errors?
            maxDelta = 1.05 * maxDelta

            SEEDPERFILE = random.randint(0, sys.maxint)
            x += 1

            csvFilename = 'syn_' + "binary" + "_" + str(rowCount) + 'x' + str(colCount) + '.csv'
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename
            csvPathnameFull = h2i.find_folder_and_filename(None, csvPathname, returnFullPath=True)

            print "Creating random", csvPathname
            write_syn_dataset(csvPathname, rowCount, colCount, expectedMin, expectedMax, SEEDPERFILE)
            parseResult = h2i.import_parse(path=csvPathname, schema='put', hex_key=hex_key, timeoutSecs=10, doSummary=False)
            print "Parse result['destination_key']:", parseResult['destination_key']

            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
            print "\n" + csvFilename

            numRows = inspect["numRows"]
            numCols = inspect["numCols"]

            summaryResult = h2o_cmd.runSummary(key=hex_key, noPrint=False, max_qbins=MAX_QBINS, numRows=numRows, numCols=numCols)
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
            h2o_util.assertApproxEqual(mins[0], expected[1], tol=maxDelta, msg='min is not approx. expected')

            maxs = stats['maxs']
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
            print ""

            print "hcnt:", hcnt
            print "len(hcnt)", len(hcnt)

            print "numRows:", numRows, "rowCount: ", rowCount
            self.assertEqual((1+NA_ROW_RATIO) * rowCount, numRows, 
                msg="numRows %s should be %s" % (numRows, (1+NA_ROW_RATIO) * rowCount))


            # don't check the last bin
            # we sometimes get a messed up histogram for all NA cols? just don't let them go thru here
            for b in hcnt[1:-1]:
                # should we be able to check for a uniform distribution in the files?
                
                e = rowCount/len(hcnt) # expect 21 thresholds, so 20 bins. each 5% of rows (uniform distribution)
                # don't check the edge bins
                # NA rows should be ignored
                self.assertAlmostEqual(b, e, delta=2*e,
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
            scipyCol = 1

            h2i.delete_keys_at_all_nodes()


if __name__ == '__main__':
    h2o.unit_main()

