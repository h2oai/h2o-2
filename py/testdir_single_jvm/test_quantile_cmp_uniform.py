import unittest, time, sys, random, math, getpass
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_import as h2i, h2o_util
import h2o_print as h2p, h2o_exec as h2e

DO_TRY_SCIPY = False
if  getpass.getuser() == 'kevin' or getpass.getuser() == 'jenkins': 
    DO_TRY_SCIPY = True

print "This test will try to compare exec quantiles to summary2 quantiles"
def twoDecimals(l): 
    if isinstance(l, list):
        return ["%.2f" % v for v in l] 
    else:
        return "%.2f" % l

# have to match the csv file?
# dtype=['string', 'float');
thresholds   = [0.001, 0.01, 0.1, 0.25, 0.33, 0.5, 0.66, 0.75, 0.9, 0.99, 0.999]

def generate_scipy_comparison(csvPathname):
    # this is some hack code for reading the csv and doing some percentile stuff in scipy
    # from numpy import loadtxt, genfromtxt, savetxt
    import numpy as np
    import scipy as sp

    dataset = np.genfromtxt(
        open(csvPathname, 'r'),
        delimiter=',',
        skip_header=1,
        dtype=None); # guess!

    print "csv read for training, done"
    # we're going to strip just the last column for percentile work
    # used below
    NUMCLASSES = 10
    print "csv read for training, done"

    # data is last column
    # drop the output
    print dataset.shape
    target = [x[1] for x in dataset]
    # we may have read it in as a string. coerce to number
    targetFP = np.array(target, np.float)

    if 1==0:
        n_features = len(dataset[0]) - 1;
        print "n_features:", n_features

        # get the end
        # target = [x[-1] for x in dataset]
        # get the 2nd col

        print "histogram of target"
        print target
        print sp.histogram(target, bins=NUMCLASSES)

        print target[0]
        print target[1]

    # per = [100 * t for t in thresholds]
    per = [1 * t for t in thresholds]
    print "sp per:", per
    from scipy import stats
    # a = stats.scoreatpercentile(target, per=per)
    a = stats.mstats.mquantiles(targetFP, prob=per)
    print "sp percentiles:", a

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
        global SEED, localhost
        SEED = h2o.setup_random_seed()
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(node_count=1, base_port=54327)
        else:
            h2o_hosts.build_cloud_with_hosts(node_count=1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_summary2_uniform(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        tryList = [
            # colname, (min, 25th, 50th, 75th, max)
            (5000000, 1, 'x.hex', 1, 20000,        ('C1',  1.10, 5000.0, 10000.0, 15000.0, 20000.00)),
            (5000000, 1, 'x.hex', -5000, 0,        ('C1', -5001.00, -3750.0, -2445, -1200.0, 99)),
            (1000000, 1, 'x.hex', -100000, 100000, ('C1',  -100001.0, -50000.0, 1613.0, 50000.0, 100000.0)),
            (1000000, 1, 'x.hex', -1, 1,           ('C1',  -1.05, -0.48, 0.0087, 0.50, 1.00)),

            (1000000, 1, 'A.hex', 1, 100,          ('C1',   1.05, 26.00, 51.00, 76.00, 100.0)),
            (1000000, 1, 'A.hex', -99, 99,         ('C1',  -99, -50.0, 0, 50.00, 99)),

            (1000000, 1, 'B.hex', 1, 10000,        ('C1',   1.05, 2501.00, 5001.00, 7501.00, 10000.00)),
            (1000000, 1, 'B.hex', -100, 100,       ('C1',  -100.10, -50.0, 0.85, 51.7, 100,00)),

            (1000000, 1, 'C.hex', 1, 100000,       ('C1',   1.05, 25002.00, 50002.00, 75002.00, 100000.00)),
            (1000000, 1, 'C.hex', -101, 101,       ('C1',  -100.10, -50.45, -1.18, 49.28, 100.00)),
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

            h2o.beta_features = False
            SEEDPERFILE = random.randint(0, sys.maxint)
            x += 1
            csvFilename = 'syn_' + "binary" + "_" + str(rowCount) + 'x' + str(colCount) + '.csv'
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename

            print "Creating random", csvPathname
            write_syn_dataset(csvPathname, rowCount, colCount, expectedMin, expectedMax, SEEDPERFILE)
            h2o.beta_features = False
            csvPathnameFull = h2i.find_folder_and_filename(None, csvPathname, returnFullPath=True)
            parseResult = h2i.import_parse(path=csvPathname, schema='put', hex_key=hex_key, timeoutSecs=10, doSummary=False)
            print "Parse result['destination_key']:", parseResult['destination_key']

            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
            print "\n" + csvFilename
            numRows = inspect["num_rows"]
            numCols = inspect["num_cols"]

            h2o.beta_features = True
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
            print "colname:", colname, "mean (2 places):", twoDecimals(mean)
            print "colname:", colname, "std dev. (2 places):", twoDecimals(sd)

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
                e = numRows/len(hcnt) # expect 21 thresholds, so 20 bins. each 5% of rows (uniform distribution)
                # don't check the edge bins
                self.assertAlmostEqual(b, rowCount/len(hcnt), delta=.01*rowCount, 
                    msg="Bins not right. b: %s e: %s" % (b, e))

            pt = twoDecimals(pctile)
            mx = twoDecimals(maxs)
            mn = twoDecimals(mins)
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
            for i, trial in enumerate(thresholds):
                execExpr = "quantile(%s[,1], c(%s));" % (hex_key, trial)
                (resultExec, result) = h2e.exec_expr(execExpr=execExpr, timeoutSecs=30)
                h2p.green_print("\nresultExec: %s" % h2o.dump_json(resultExec))
                ex = twoDecimals(result)
                h2p.blue_print("\nthreshold: %.2f Exec quantile: %s Summary2: %s" % (trial, ex, pt[i]))
                h2o_util.assertApproxEqual(result, pctile[i], tol=maxDelta, msg='percentile: % is not expected: %s' % (result, pctile[i]))

            if DO_TRY_SCIPY:
                generate_scipy_comparison(csvPathnameFull)

if __name__ == '__main__':
    h2o.unit_main()

