import unittest, time, sys, random, math, getpass
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_import as h2i, h2o_util, h2o_print as h2p, h2o_summ

print "Here we pass a list of data values. The dataset is just those data values"
print "useful for analyzing small datasets, like with 3 values (ints reals, whatever)"
print "Actually, we'll pass rowCount, if not None, we'll do random.choice from the 3 values for that RowCount"

DO_TRY_SCIPY = False
if getpass.getuser()=='kevin' or getpass.getuser()=='jenkins':
    DO_TRY_SCIPY = True

DO_MEDIAN = True
MAX_QBINS = 1000
COLS = 100000

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
        # skip_header=1,
        dtype=None); # guess!

    print "csv read for training, done"
    # we're going to strip just the last column for percentile work
    # used below
    NUMCLASSES = 10
    print "csv read for training, done"

    # data is last column
    # drop the output
    print dataset.shape
    if len(dataset.shape) > 1:
        target = [x[col] for x in dataset]
    else:
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
    print "scipy per:", thresholds
    from scipy import stats
    # a = stats.scoreatpercentile(target, per=per)
    a = stats.mstats.mquantiles(targetFP, prob=thresholds)
    a2 = ["%.2f" % v for v in a]
    h2p.red_print("scipy stats.mstats.mquantiles:", a2)

    # also get the median with a painful sort (h2o_summ.percentileOnSortedlist()
    # inplace sort
    targetFP.sort()
    b = h2o_summ.percentileOnSortedList(targetFP, 0.50 if DO_MEDIAN else 0.999, interpolate='linear')
    label = '50%' if DO_MEDIAN else '99.9%'
    h2p.blue_print(label, "from sort:", b)
    s = a[5 if DO_MEDIAN else 10]
    h2p.blue_print(label, "from scipy:", s)
    h2p.blue_print(label, "from h2o summary2:", h2oMedian)
    h2p.blue_print(label, "from h2o quantile multipass:", h2oMedian2)
    # they should be identical. keep a tight absolute tolerance
    h2o_util.assertApproxEqual(h2oMedian2, b, tol=0.0000002, msg='h2o quantile multipass is not approx. same as sort algo')
    h2o_util.assertApproxEqual(h2oMedian2, s, tol=0.0000002, msg='h2o quantile multipass is not approx. same as scipy algo')
    # see if scipy changes. nope. it doesn't 
    if 1==0:
        a = stats.mstats.mquantiles(targetFP, prob=per, alphab=1, betab=1)
        a2 = ["%.2f" % v for v in a]
        h2p.red_print("after sort")
        h2p.red_print("scipy stats.mstats.mquantiles:", a2)


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
        global SEED, localhost
        SEED = h2o.setup_random_seed()
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(node_count=1, java_heap_GB=16, base_port=54327)
        else:
            h2o_hosts.build_cloud_with_hosts(node_count=1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_summary2_small(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        tryList = [
            # colname, (min, 25th, 50th, 75th, max)
            # if rowCount is None, we'll just use  the data values
            # None in expected values means no compare
            (None, 1, 'x.hex', [-1,0,1],        ('C1',  None, None, -1, None, None)),
            (None, 2, 'x.hex', [-1,0,1],        ('C1',  None, None, -1, None, None)),
            (None, 10, 'x.hex', [-1,0,1],        ('C1',  None, None, -1, None, None)),
            (None, 100, 'x.hex', [-1,0,1],        ('C1',  None, None, -1, None, None)),
            (None, 1000, 'x.hex', [-1,0,1],        ('C1',  None, None, -1, None, None)),
            (None, 10000, 'x.hex', [-1,0,1],        ('C1',  None, None, -1, None, None)),
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

            h2o.beta_features = False
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

            h2o.beta_features = False
            csvPathnameFull = h2i.find_folder_and_filename(None, csvPathname, returnFullPath=True)
            parseResult = h2i.import_parse(path=csvPathname, schema='put', hex_key=hex_key, timeoutSecs=30, doSummary=False)
            print "Parse result['destination_key']:", parseResult['destination_key']

            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
            print "\n" + csvFilename

            numRows = inspect["num_rows"]
            numCols = inspect["num_cols"]

            h2o.beta_features = True
            summaryResult = h2o_cmd.runSummary(key=hex_key, max_qbins=MAX_QBINS, timeoutSecs=45)
            h2o.verboseprint("summaryResult:", h2o.dump_json(summaryResult))

            quantile = 0.5 if DO_MEDIAN else .999
            q = h2o.nodes[0].quantiles(source_key=hex_key, column=0, interpolation_type=7,
                quantile=quantile, max_qbins=MAX_QBINS, multiple_pass=1)
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

            print "colname:", colname, "mean (2 places):", twoDecimals(mean)
            print "colname:", colname, "std dev. (2 places):", twoDecimals(sd)

            zeros = stats['zeros']
            mins = stats['mins']
            maxs = stats['maxs']
            pct = stats['pct']
            # the thresholds h2o used, should match what we expected
            expectedPct= [0.01, 0.05, 0.1, 0.25, 0.33, 0.5, 0.66, 0.75, 0.9, 0.95, 0.99]

            pctile = stats['pctile']
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
                     # h2oMedian=pctile[5 if DO_MEDIAN else 10], result_single)
                    h2oMedian=pctile[5 if DO_MEDIAN else 10], h2oMedian2=qresult)



            h2i.delete_keys_at_all_nodes()


if __name__ == '__main__':
    h2o.unit_main()

