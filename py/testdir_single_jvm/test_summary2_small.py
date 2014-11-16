import unittest, time, sys, random, math, getpass
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_import as h2i, h2o_util, h2o_print as h2p, h2o_summ

print "Here we pass a list of data values. The dataset is just those data values"
print "useful for analyzing small datasets, like with 3 values (ints reals, whatever)"
print "Actually, we'll pass rowCount, if not None, we'll do random.choice from the 3 values for that RowCount"

DO_TRY_SCIPY = False
if getpass.getuser()=='kevin' or getpass.getuser()=='jenkins':
    DO_TRY_SCIPY = True

DO_MEDIAN = True
MAX_QBINS = 1000

def write_syn_dataset(csvPathname, rowCount, colCount, values, SEED):
    r1 = random.Random(SEED)
    dsf = open(csvPathname, "w+")

    if not rowCount:
        # one row for each value. same value in each col
        for v in values:
            rowData = [v for c in range(colCount)]
            rowDataCsv = ",".join(map(str,rowData))
            dsf.write(rowDataCsv + "\n")
    else:
        for i in range(rowCount):
            rowData = [random.choice(values) for c in range (colCount)]
            rowDataCsv = ",".join(map(str,rowData))
            dsf.write(rowDataCsv + "\n")

    dsf.close()

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init(java_heap_GB=16)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_summary2_small(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        tryList = [
            # colname, (min, 25th, 50th, 75th, max)
            # if rowCount is None, we'll just use  the data values
            # None in expected values means no compare
            (None, 1, 'x.hex', [-1,0,1],        ('C1',  None, None, 0, None, None)),
            (None, 2, 'x.hex', [-1,0,1],        ('C1',  None, None, 0, None, None)),
            (None, 10, 'x.hex', [-1,0,1],        ('C1',  None, None, 0, None, None)),
            (None, 100, 'x.hex', [-1,0,1],        ('C1',  None, None, 0, None, None)),
            (None, 1000, 'x.hex', [-1,0,1],        ('C1',  None, None, 0, None, None)),
            # (None, 10000, 'x.hex', [-1,0,1],        ('C1',  None, None, 0, None, None)),
            # (COLS, 1, 'x.hex', [1,0,-1],        ('C1',  None, None, None, None, None)),
        ]

        timeoutSecs = 10
        trial = 1
        n = h2o.nodes[0]
        lenNodes = len(h2o.nodes)

        x = 0
        timeoutSecs = 60
        for (rowCount, colCount, hex_key, values, expected) in tryList:
            # max error = half the bin size?
        
            expectedMax = max(values)
            expectedMin = min(values)
            maxDelta = ((expectedMax - expectedMin)/20.0) / 2.0
            # add 5% for fp errors?
            maxDelta = 1.05 * maxDelta

            # hmm...say we should be 100% accurate for these tests?
            maxDelta = 0

            SEEDPERFILE = random.randint(0, sys.maxint)
            x += 1

            if not rowCount:
                rowFile = len(values)
            else:
                rowFile = rowCount
            csvFilename = 'syn_' + "binary" + "_" + str(rowFile) + 'x' + str(colCount) + '.csv'
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename

            print "Creating random", csvPathname
            write_syn_dataset(csvPathname, rowCount, colCount, values, SEEDPERFILE)

            csvPathnameFull = h2i.find_folder_and_filename(None, csvPathname, returnFullPath=True)
            parseResult = h2i.import_parse(path=csvPathname, schema='put', hex_key=hex_key, timeoutSecs=30, doSummary=False)
            print "Parse result['destination_key']:", parseResult['destination_key']

            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
            print "\n" + csvFilename

            numRows = inspect["numRows"]
            numCols = inspect["numCols"]

            summaryResult = h2o_cmd.runSummary(key=hex_key, max_qbins=MAX_QBINS, timeoutSecs=45)
            h2o.verboseprint("summaryResult:", h2o.dump_json(summaryResult))

            quantile = 0.5 if DO_MEDIAN else .999
            q = h2o.nodes[0].quantiles(source_key=hex_key, column=0, interpolation_type=7,
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

            # only one column
            column = summaryResult['summaries'][0]

            colname = column['colname']

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
            pct = stats['pct']
            # the thresholds h2o used, should match what we expected
            expectedPct= [0.01, 0.05, 0.1, 0.25, 0.33, 0.5, 0.66, 0.75, 0.9, 0.95, 0.99]

            pctile = stats['pctile']
            print "pctile:", pctile
            if expected[0]:
                self.assertEqual(colname, expected[0])
            if expected[1]:
                h2o_util.assertApproxEqual(mins[0], expected[1], tol=maxDelta, msg='min is not approx. expected')
            if expected[2]:
                h2o_util.assertApproxEqual(pctile[3], expected[2], tol=maxDelta, msg='25th percentile is not approx. expected')
            if expected[3]:
                h2o_util.assertApproxEqual(pctile[5], expected[3], tol=maxDelta, msg='50th percentile (median) is not approx. expected')
            if expected[4]:
                h2o_util.assertApproxEqual(pctile[7], expected[4], tol=maxDelta, msg='75th percentile is not approx. expected')
            if expected[5]:
                h2o_util.assertApproxEqual(maxs[0], expected[5], tol=maxDelta, msg='max is not approx. expected')

            hstart = column['hstart']
            hstep = column['hstep']
            hbrk = column['hbrk']
            hcnt = column['hcnt']

            print "pct:", pct
            print ""

            print "hcnt:", hcnt
            print "len(hcnt)", len(hcnt)

            # don't check the last bin
            for b in hcnt[1:-1]:
                # should we be able to check for a uniform distribution in the files?
                e = numRows/len(hcnt) # expect 21 thresholds, so 20 bins. each 5% of rows (uniform distribution)
                # don't check the edge bins
                self.assertAlmostEqual(b, numRows/len(hcnt), delta=1 + .01*numRows,
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
            h2o.nodes[0].remove_all_keys()

            scipyCol = 0

            # don't check if colname is empty..means it's a string and scipy doesn't parse right?
            if colname!='':
                # don't do for enums
                # also get the median with a sort (h2o_summ.percentileOnSortedlist()
                h2o_summ.quantile_comparisons(
                    csvPathnameFull,
                    col=scipyCol, # what col to extract from the csv
                    datatype='float',
                    quantile=0.5 if DO_MEDIAN else 0.999,
                    h2oSummary2=pctile[5 if DO_MEDIAN else 10],
                    # h2oQuantilesApprox=qresult_single,
                    h2oQuantilesExact=qresult,
                    )


if __name__ == '__main__':
    h2o.unit_main()

