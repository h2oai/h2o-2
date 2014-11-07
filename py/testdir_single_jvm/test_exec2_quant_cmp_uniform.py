import unittest, time, sys, random, math, getpass
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_import as h2i, h2o_util
import h2o_print as h2p, h2o_exec as h2e, h2o_summ


# have to match the csv file?
# dtype=['string', 'float');
thresholds   = [0.001, 0.01, 0.1, 0.25, 0.33, 0.5, 0.66, 0.75, 0.9, 0.99, 0.999]
thresholds   = [0.0]

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

    def test_exec2_quant_cmp_uniform(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        tryList = [
            # colname, (min, 25th, 50th, 75th, max)
            (500000, 1, 'x.hex', 1, 20000,        ('C1',  1.10, 5000.0, 10000.0, 15000.0, 20000.00)),
            (500000, 1, 'x.hex', -5000, 0,        ('C1', -5001.00, -3750.0, -2445, -1200.0, 99)),
            (100000, 1, 'x.hex', -100000, 100000, ('C1',  -100001.0, -50000.0, 1613.0, 50000.0, 100000.0)),
            (100000, 1, 'x.hex', -1, 1,           ('C1',  -1.05, -0.48, 0.0087, 0.50, 1.00)),

            (100000, 1, 'A.hex', 1, 100,          ('C1',   1.05, 26.00, 51.00, 76.00, 100.0)),
            (100000, 1, 'A.hex', -99, 99,         ('C1',  -99, -50.0, 0, 50.00, 99)),

            (100000, 1, 'B.hex', 1, 10000,        ('C1',   1.05, 2501.00, 5001.00, 7501.00, 10000.00)),
            (100000, 1, 'B.hex', -100, 100,       ('C1',  -100.10, -50.0, 0.85, 51.7, 100,00)),

            (100000, 1, 'C.hex', 1, 100000,       ('C1',   1.05, 25002.00, 50002.00, 75002.00, 100000.00)),
            (100000, 1, 'C.hex', -101, 101,       ('C1',  -100.10, -50.45, -1.18, 49.28, 100.00)),
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

            print "Creating random", csvPathname
            write_syn_dataset(csvPathname, rowCount, colCount, expectedMin, expectedMax, SEEDPERFILE)
            csvPathnameFull = h2i.find_folder_and_filename(None, csvPathname, returnFullPath=True)
            parseResult = h2i.import_parse(path=csvPathname, schema='put', hex_key=hex_key, timeoutSecs=30, doSummary=False)
            print "Parse result['destination_key']:", parseResult['destination_key']

            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
            print "\n" + csvFilename
            numRows = inspect["numRows"]
            numCols = inspect["numCols"]

            summaryResult = h2o_cmd.runSummary(key=hex_key)
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
            expectedPct = [0.001, 0.01, 0.1, 0.25, 0.33, 0.5, 0.66, 0.75, 0.9, 0.99, 0.999]
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
                e = numRows/len(hcnt) 
                # apparently we're not able to estimate for these datasets
                # self.assertAlmostEqual(b, rowCount/len(hcnt), delta=.01*rowCount, 
                #     msg="Bins not right. b: %s e: %s" % (b, e))

            pt = h2o_util.twoDecimals(pctile)
            mx = h2o_util.twoDecimals(maxs)
            mn = h2o_util.twoDecimals(mins)
            print "colname:", colname, "pctile (2 places):", pt
            print "colname:", colname, "maxs: (2 places):", mx
            print "colname:", colname, "mins: (2 places):", mn

            # FIX! we should do an exec and compare using the exec quantile too
            compareActual = mn[0], pt[3], pt[5], pt[7], mx[0]
            print "min/25/50/75/max colname:", colname, "(2 places):", compareActual
            print "maxs colname:", colname, "(2 places):", mx
            print "mins colname:", colname, "(2 places):", mn

            trial += 1
            h2p.blue_print("\nTrying exec quantile")
            # thresholds = "c(0.01, 0.05, 0.1, 0.25, 0.33, 0.5, 0.66, 0.75, 0.9, 0.95, 0.99)"
            # do the equivalent exec quantile?
            # execExpr = "quantile(%s[,1],%s);" % (hex_key, thresholds)

            print "Comparing (two places) each of the summary2 threshold quantile results, to single exec quantile"
            for i, threshold in enumerate(thresholds):
                # FIX! do two of the same?..use same one for the 2nd
                if i!=0:
                    # execExpr = "r2=c(1); r2=quantile(%s[,4],c(0,.05,0.3,0.55,0.7,0.95,0.99))" % hex_key
                    execExpr = "r2=c(1); r2=quantile(%s[,1], c(%s,%s));" % (hex_key, threshold, threshold)
                    (resultExec, result) = h2e.exec_expr(execExpr=execExpr, timeoutSecs=30)
                    h2p.green_print("\nresultExec: %s" % h2o.dump_json(resultExec))
                    h2p.blue_print("\nthreshold: %.2f Exec quantile: %s Summary2: %s" % (threshold, result, pt[i]))
                    if not result:
                        raise Exception("exec result: %s for quantile: %s is bad" % (result, threshold))
                    h2o_util.assertApproxEqual(result, pctile[i], tol=maxDelta, 
                        msg='exec percentile: %s too different from expected: %s' % (result, pctile[i]))
                # for now, do one with all, but no checking
                else:
                    # This seemed to "work" but how do I get the key name for the list of values returned
                    # the browser result field seemed right, but nulls in the key
                    if 1==0:
                        execExpr = "r2=c(1); r2=quantile(%s[,1], c(%s));" % (hex_key, ",".join(map(str,thresholds)))
                    else:
                        # does this way work (column getting)j
                        execExpr = "r2=c(1); r2=quantile(%s$C1, c(%s));" % (hex_key, ",".join(map(str,thresholds)))
                    (resultExec, result) = h2e.exec_expr(execExpr=execExpr, timeoutSecs=30)
                    inspect = h2o_cmd.runInspect(key='r2') 
                    numCols = inspect['numCols']
                    numRows = inspect['numRows']

                    self.assertEqual(numCols,1)
                    self.assertEqual(numRows,len(thresholds))
                    # FIX! should run thru the values in the col? how to get

            # compare the last one
            if colname!='':
                # don't do for enums
                # also get the median with a sort (h2o_summ.percentileOnSortedlist()
                h2o_summ.quantile_comparisons(
                    csvPathnameFull,
                    col=0, # what col to extract from the csv
                    datatype='float',
                    quantile=thresholds[-1],
                    # h2oSummary2=pctile[-1],
                    # h2oQuantilesApprox=result, # from exec
                    h2oExecQuantiles=result,
                    )

            h2o.nodes[0].remove_all_keys()


if __name__ == '__main__':
    h2o.unit_main()

