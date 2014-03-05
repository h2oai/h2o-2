import unittest, time, sys, random, math, getpass
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_import as h2i, h2o_util, h2o_print as h2p
import h2o_summ

print "Like test_summary_uniform, but with integers only"
print "focuses on numbers from 2B to 3B, which seem to have been dropped by another test?"


DO_MEDIAN = False
DO_TRY_SCIPY = False
if  getpass.getuser() == 'kevin': 
    DO_TRY_SCIPY = True

MAX_QBINS = 1
MAX_QBINS = 1000000
DO_REAL = False

def twoDecimals(l): 
    if isinstance(l, list):
        return ["%.2f" % v for v in l] 
    else:
        return "%.2f" % l

# have to match the csv file?
# dtype=['string', 'float');
def generate_scipy_comparison(csvPathname, col, h2oMedian):
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
    # single col dataset?
    target = dataset
    # target = [x[col] for x in dataset]

    # we may have read it in as a string. coerce to number
    # targetFP = np.array(target, np.float)
    targetFP = target

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
    h2p.blue_print(label, "from h2o:", h2oMedian)

    # see if scipy changes. nope. it doesn't 
    if 1==0:
        a = stats.mstats.mquantiles(targetFP, prob=per)
        a2 = ["%.2f" % v for v in a]
        h2p.red_print("after sort")
        h2p.red_print("scipy stats.mstats.mquantiles:", a2)



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

    def test_summary2_int2B(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        tryList = [
            # colname, (min, 25th, 50th, 75th, max)
            (100000, 1, 'B.hex', 2533255332, 2633256000,   ('C1',  None, None, None, None, None)),
        ]

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

            h2o.beta_features = False
            SEEDPERFILE = random.randint(0, sys.maxint)
            x += 1

            csvFilename = 'syn_' + "binary" + "_" + str(rowCount) + 'x' + str(colCount) + '.csv'
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename

            print "Creating random", csvPathname
            write_syn_dataset(csvPathname, rowCount, colCount, expectedMin, expectedMax, SEEDPERFILE)
            h2o.beta_features = False
            csvPathnameFull = h2i.find_folder_and_filename(None, csvPathname, returnFullPath=True)
            parseResult = h2i.import_parse(path=csvPathname, schema='put', hex_key=hex_key, timeoutSecs=60, doSummary=False)
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
            if expected[0]:
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
            maxs = stats['maxs']

            pct = stats['pct']
            # the thresholds h2o used, should match what we expected
            expectedPct= [0.01, 0.05, 0.1, 0.25, 0.33, 0.5, 0.66, 0.75, 0.9, 0.95, 0.99]

            pctile = stats['pctile']
            if expected[1]:
                h2o_util.assertApproxEqual(mins[0], expected[1], tol=maxDelta, msg='min is not approx. expected')
                h2o_util.assertApproxEqual(pctile[3], expected[2], tol=maxDelta, msg='25th percentile is not approx. expected')
                h2o_util.assertApproxEqual(pctile[5], expected[3], tol=maxDelta, msg='50th percentile (median) is not approx. expected')
                h2o_util.assertApproxEqual(pctile[7], expected[4], tol=maxDelta, msg='75th percentile is not approx. expected')

                h2o_util.assertApproxEqual(maxs[0], expected[5], tol=maxDelta, msg='max is not approx. expected')

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

            scipyCol = 0 
            if DO_TRY_SCIPY:
                csvPathname1 = h2i.find_folder_and_filename(None, csvPathname, returnFullPath=True)
                # always use the last col here
                # also get the median with a painful sort (h2o_summ.percentileOnSortedlist()
                generate_scipy_comparison(csvPathnameFull, col=scipyCol,
                            h2oMedian=pctile[5 if DO_MEDIAN else 10])


if __name__ == '__main__':
    h2o.unit_main()

