import unittest, time, sys, random, math, getpass
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_import as h2i, h2o_util, h2o_print as h2p, h2o_summ

DO_TRY_SCIPY = False
if getpass.getuser()=='kevin' or getpass.getuser()=='jenkins':
    DO_TRY_SCIPY = True

DO_MEDIAN = False
MAX_QBINS = 1000000
MAX_QBINS = 100000
ROWS = 100000

def twoDecimals(l): 
    if isinstance(l, list):
        return ["%.2f" % v for v in l] 
    else:
        return "%.2f" % l

def generate_scipy_comparison(csvPathname, col=0, h2oMedian=None, h2oMedian2=None):
    # this is some hack code for reading the csv and doing some percentile stuff in scipy
    # from numpy import loadtxt, genfromtxt, savetxt
    import numpy as np
    import scipy as sp

    dataset = np.genfromtxt(
        open(csvPathname, 'r'),
        delimiter=',',
        # skip_header=1, // deadly if I skip a row on accuracy. shows up with linear interpolation.
        dtype=None); # guess!

    print "csv read for training, done"
    # we're going to strip just the last column for percentile work
    # used below
    NUMCLASSES = 10
    print "csv read for training, done"

    # data is last column
    # drop the output
    print dataset.shape
    # target = [x[col] for x in dataset]
    target = dataset
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

    thresholds   = [0.001, 0.01, 0.1, 0.25, 0.33, 0.5, 0.66, 0.75, 0.9, 0.99, 0.999]
    # http://docs.scipy.org/doc/numpy-dev/reference/generated/numpy.percentile.html
    # numpy.percentile has simple linear interpolate and midpoint
    # need numpy 1.9 for interpolation. numpy 1.8 doesn't have
    # p = np.percentile(targetFP, 50 if DO_MEDIAN else 99.9, interpolation='midpoint')
    # 1.8
    p = np.percentile(targetFP, 50 if DO_MEDIAN else 99.9)
    h2p.red_print("numpy.percentile", p)

    # per = [100 * t for t in thresholds]
    from scipy import stats
    a = stats.scoreatpercentile(targetFP, per=50 if DO_MEDIAN else 99.9)
    h2p.red_print("scipy stats.scoreatpercentile", a)

    # scipy apparently doesn't have the use of means (type 2)
    # http://en.wikipedia.org/wiki/Quantile
    # it has median (R-8) with 1/3, 1/3

    # type 6
    alphap=0
    betap=0

    # type 5 okay but not perfect
    alphap=0.5
    betap=0.5

    # type 8
    alphap=1/3.0
    betap=1/3.0

    # an approx? (was good when comparing to h2o type 2)
    alphap=0.4
    betap=0.4

    # this is type 7
    alphap=1
    betap=1

    per = [1 * t for t in thresholds]
    print "scipy per", per
    a = stats.mstats.mquantiles(targetFP, prob=per, alphap=alphap, betap=betap)
    # http://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.mstats.mquantiles.html
    # type 7 
    # alphap=0.4, betap=0.4, 
    # type 2 not available? (mean)
    # alphap=1/3.0, betap=1/3.0 is approx median?

    a2 = [v for v in a]
    h2p.red_print("scipy stats.mstats.mquantiles type 8 (don't have type 2):", a2)

    # also get the median with a painful sort (h2o_summ.percentileOnSortedlist()
    # inplace sort
    targetFP.sort()

    # this matches scipy type 7 (linear)
    # b = h2o_summ.percentileOnSortedList(targetFP, 0.50 if DO_MEDIAN else 0.999, interpolate='linear')
    # this matches h2o type 2 (mean)
    # b = h2o_summ.percentileOnSortedList(targetFP, 0.50 if DO_MEDIAN else 0.999, interpolate='mean')
    b = h2o_summ.percentileOnSortedList(targetFP, 0.50 if DO_MEDIAN else 0.999, interpolate='linear')
    label = '50%' if DO_MEDIAN else '99.9%'
    h2p.blue_print(label, "from sort:", b)
    s = a[5 if DO_MEDIAN else 10]
    h2p.blue_print(label, "from scipy:", s)
    h2p.blue_print(label, "from numpy:", p)
    h2p.blue_print(label, "from h2o summary:", h2oMedian)
    h2p.blue_print(label, "from h2o multipass:", h2oMedian2)
    # they should be identical. keep a tight absolute tolerance
    h2o_util.assertApproxEqual(h2oMedian2, b, tol=0.0000002, msg='h2o quantile multipass is not approx. same as sort algo')
    h2o_util.assertApproxEqual(h2oMedian2, p, rel=0.01, msg='h2o quantile multipass is not approx. same as numpy algo')
    # give us some slack compared to the scipy use of median
    h2o_util.assertApproxEqual(h2oMedian2, s, rel=0.01, msg='h2o quantile multipass is not approx. same as scipy algo')

    # see if scipy changes. nope. it doesn't 
    if 1==0:
        a = stats.mstats.mquantiles(targetFP, prob=per, alphap=alphap, betap=betap)
        a2 = [v for v in a]
        h2p.red_print("after sort")
        h2p.red_print("scipy stats.mstats.mquantiles:", a2)

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
            h2o.build_cloud(node_count=3, base_port=54327)
        else:
            h2o_hosts.build_cloud_with_hosts(node_count=1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_summary2_uniform(self):
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

            h2o.beta_features = False
            SEEDPERFILE = random.randint(0, sys.maxint)
            x += 1

            csvFilename = 'syn_' + "binary" + "_" + str(rowCount) + 'x' + str(colCount) + '.csv'
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename

            print "Creating random", csvPathname
            write_syn_dataset(csvPathname, rowCount, colCount, expectedMin, expectedMax, SEEDPERFILE)
            h2o.beta_features = False
            csvPathnameFull = h2i.find_folder_and_filename(None, csvPathname, returnFullPath=True)
            parseResult = h2i.import_parse(path=csvPathname, schema='put', hex_key=hex_key, timeoutSecs=30, doSummary=False)
            print "Parse result['destination_key']:", parseResult['destination_key']

            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
            print "\n" + csvFilename

            numRows = inspect["num_rows"]
            numCols = inspect["num_cols"]
            h2o.beta_features = True
            summaryResult = h2o_cmd.runSummary(key=hex_key, max_qbins=MAX_QBINS)
            h2o.verboseprint("summaryResult:", h2o.dump_json(summaryResult))

            # only one column
            column = summaryResult['summaries'][0]
            colname = column['colname']
            self.assertEqual(colname, expected[0])

            quantile = 0.5 if DO_MEDIAN else .999
            q = h2o.nodes[0].quantiles(source_key=hex_key, column=column['colname'],
                quantile=quantile, max_qbins=MAX_QBINS, multiple_pass=1, interpolation_type=7) # linear
            qresult = q['result']
            qresult_single = q['result_single']
            h2p.blue_print("h2o quantiles result:", qresult)
            h2p.blue_print("h2o quantiles result_single:", qresult_single)
            h2p.blue_print("h2o quantiles iterations:", q['iterations'])
            h2p.blue_print("h2o quantiles interpolated:", q['interpolated'])
            print h2o.dump_json(q)

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

            pt = twoDecimals(pctile)
            mx = twoDecimals(maxs)
            mn = twoDecimals(mins)
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
            if DO_TRY_SCIPY and colname!='':
                # don't do for enums
                # also get the median with a sort (h2o_summ.percentileOnSortedlist()
                print scipyCol, pctile[10]
                generate_scipy_comparison(csvPathnameFull, col=scipyCol,
                    h2oMedian=pctile[5 if DO_MEDIAN else 10], h2oMedian2=qresult)



if __name__ == '__main__':
    h2o.unit_main()

