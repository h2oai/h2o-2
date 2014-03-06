import unittest, time, sys, random, math, getpass
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_import as h2i, h2o_util, h2o_print as h2p, h2o_summ

print "Same as test_summary2_uniform.py but with exponential distribution on the data"
DO_TRY_SCIPY = False
if  getpass.getuser() == 'kevin':
    DO_TRY_SCIPY = True

DO_MEDIAN = True
MAX_QBINS = 1000000
# 1 over desired mean
LAMBD = 0.005

def twoDecimals(l): 
    if isinstance(l, list):
        return ["%.2f" % v for v in l] 
    else:
        return "%.2f" % l

def generate_scipy_comparison(csvPathname, col=0, h2oMedian=None, h2o1Median=None):
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
    # per = [100 * t for t in thresholds]
    per = [1 * t for t in thresholds]
    print "scipy per:", per
    from scipy import stats
    # a = stats.scoreatpercentile(target, per=per)
    a = stats.mstats.mquantiles(targetFP, prob=per)
    a2 = ["%.2f" % v for v in a]
    h2p.red_print("scipy stats.mstats.mquantiles:", a2)

    # also get the median with a painful sort (h2o_summ.percentileOnSortedlist()
    # inplace sort
    targetFP.sort()
    b = h2o_summ.percentileOnSortedList(targetFP, 0.50 if DO_MEDIAN else 0.999)
    label = '50%' if DO_MEDIAN else '99.9%'
    h2p.blue_print(label, "from sort:", b)
    h2p.blue_print(label, "from scipy:", a[5 if DO_MEDIAN else 10])
    h2p.blue_print(label, "from h2o2:", h2oMedian)
    # Summary1 has a different threshold ..not 99.9%
    # median is okay
    if DO_MEDIAN:
        h2p.blue_print(label, "from h2o1:", h2o1Median)
    # see if scipy changes. nope. it doesn't 
    if 1==0:
        a = stats.mstats.mquantiles(targetFP, prob=per)
        a2 = ["%.2f" % v for v in a]
        h2p.red_print("after sort")
        h2p.red_print("scipy stats.mstats.mquantiles:", a2)

def write_syn_dataset(csvPathname, rowCount, colCount, lambd=0.2, SEED=None):
    r1 = random.Random(SEED)
    dsf = open(csvPathname, "w+")

    expectedMin = None
    expectedMax = None
    for i in range(rowCount):
        rowData = []

        # Exponential distribution. 
        # lambd is 1.0 divided by the desired mean. It should be nonzero. 
        # Returned values range from 0 to positive infinity if lambd is positive, 
        # and from negative infinity to 0 if lambd is negative.
        for j in range(colCount):
            ri = random.expovariate(lambd=lambd)
            # None doesn't dominate for max, it doesn for min
            if expectedMin is None:
                expectedMin = ri
            else:
                expectedMin = min(expectedMin, ri)

            if expectedMax is None:
                expectedMax = ri
            else:
                expectedMax = max(expectedMax, ri)
            rowData.append(ri)

        rowDataCsv = ",".join(map(str,rowData))
        dsf.write(rowDataCsv + "\n")
        

    dsf.close()
    return (expectedMin, expectedMax)

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

    def test_summary2_exp(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        LAMBD = random.uniform(0.005, 0.5)
        tryList = [
            # colname, (min, 25th, 50th, 75th, max)
            (10,     1, 'x.hex', 1, 20000,        ('C1', None, None, None, None, None)),
            (100,    1, 'x.hex', 1, 20000,        ('C1', None, None, None, None, None)),
            (1000,   1, 'x.hex', -5000, 0,        ('C1', None, None, None, None, None)),
            (10000,  1, 'x.hex', -100000, 100000, ('C1', None, None, None, None, None)),
            (100000, 1, 'x.hex', -1, 1,           ('C1', None, None, None, None, None)),
            (1000000, 1, 'A.hex', 1, 100,          ('C1', None, None, None, None, None)),
        ]

        timeoutSecs = 10
        trial = 1
        n = h2o.nodes[0]
        lenNodes = len(h2o.nodes)

        x = 0
        timeoutSecs = 60
        # rangeMin and rangeMax are not used right now
        for (rowCount, colCount, hex_key, rangeMin, rangeMax, expected) in tryList:
            h2o.beta_features = False
            SEEDPERFILE = random.randint(0, sys.maxint)
            x += 1

            csvFilename = 'syn_' + "binary" + "_" + str(rowCount) + 'x' + str(colCount) + '.csv'
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename

            print "Creating random", csvPathname, "lambd:", LAMBD
            (expectedMin, expectedMax) = write_syn_dataset(csvPathname, 
                rowCount, colCount, lambd=LAMBD, SEED=SEEDPERFILE)
            print "expectedMin:", expectedMin, "expectedMax:", expectedMax
            maxDelta = ((expectedMax - expectedMin)/20.0) / 2.0
            # add 5% for fp errors?
            maxDelta = 1.05 * maxDelta

            h2o.beta_features = False
            csvPathnameFull = h2i.find_folder_and_filename(None, csvPathname, returnFullPath=True)
            parseResult = h2i.import_parse(path=csvPathname, schema='put', hex_key=hex_key, timeoutSecs=30, doSummary=False)
            print "Parse result['destination_key']:", parseResult['destination_key']

            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
            print "\n" + csvFilename

            numRows = inspect["num_rows"]
            numCols = inspect["num_cols"]

            h2o.beta_features = False
            summary1Result = h2o_cmd.runSummary(key=hex_key)
            h2o.verboseprint("Summary1 summary1Result:", h2o.dump_json(summary1Result))
            percentiles1 = summary1Result['summary']['columns'][0]['percentiles']
            thresholds1 = percentiles1['thresholds']
            values1 = percentiles1['values']

            print "Summary1 thresholds", twoDecimals(thresholds1)
            print "Summary1 values", twoDecimals(values1)

            h2o.beta_features = True
            summaryResult = h2o_cmd.runSummary(key=hex_key, max_qbins=MAX_QBINS)
            h2o.verboseprint("Summary2 summaryResult:", h2o.dump_json(summaryResult))

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

            print "colname:", colname, "mean (2 places):", twoDecimals(mean)
            print "colname:", colname, "std dev. (2 places):", twoDecimals(sd)

            zeros = stats['zeros']
            mins = stats['mins']
            maxs = stats['maxs']
            pct = stats['pct']
            expectedPct= [0.01, 0.05, 0.1, 0.25, 0.33, 0.5, 0.66, 0.75, 0.9, 0.95, 0.99]
            pctile = stats['pctile']
            # the thresholds h2o used, should match what we expected
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

            print "Can't estimate the bin distribution"

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
                    h2oMedian=pctile[5 if DO_MEDIAN else 10], 
                    h2o1Median=values1[5 if DO_MEDIAN else 10]) # from Summary1



if __name__ == '__main__':
    h2o.unit_main()

