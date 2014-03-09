import unittest, time, sys, random, math, getpass
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_import as h2i, h2o_util, h2o_browse as h2b, h2o_print as h2p
import h2o_summ

print "same as test_summary2_unifiles.py but using local runif_.csv single col for comparison testing"
DO_TRY_SCIPY = False
if  getpass.getuser() == 'kevin':
    DO_TRY_SCIPY = True

DO_MEDIAN = True
MAX_QBINS = 1000

def twoDecimals(l):
    if isinstance(l, list):
        return ["%.2f" % v for v in l]
    else:
        return "%.2f" % l

# have to match the csv file?
# dtype=['string', 'float');
def generate_scipy_comparison(csvPathname, col=0, h2oMedian=None, h2oMedian2=None, csvFilename=None):
    # this is some hack code for reading the csv and doing some percentile stuff in scipy
    # from numpy import loadtxt, genfromtxt, savetxt
    import numpy as np
    import scipy as sp

    dataset = np.genfromtxt(
        open(csvPathname, 'r'),
        delimiter=',',
        skip_header=0, # no header!
        dtype=None); # guess!

    print "csv read for training, done"
    # we're going to strip just the last column for percentile work
    # used below
    NUMCLASSES = 10
    print "csv read for training, done"

    # data is last column
    # drop the output
    print dataset.shape
    print csvFilename
    if csvFilename == 'runif_.csv':
        target = dataset
    else:
        target = [x[col] for x in dataset]

    # target = dataset
    # we may have read it in as a string. coerce to number
    targetFP = np.array(target, np.float)
    # targetFP = target

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
    h2p.blue_print(label, "from h2o singlepass:", h2oMedian)
    h2p.blue_print(label, "from h2o multipass:", h2oMedian2)
    # see if scipy changes. nope. it doesn't
    if 1==0:
        a = stats.mstats.mquantiles(targetFP, prob=per)
        a2 = ["%.2f" % v for v in a]
        h2p.red_print("after sort")
        h2p.red_print("scipy stats.mstats.mquantiles:", a2)


class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED, localhost
        SEED = h2o.setup_random_seed()
        localhost = h2o.decide_if_localhost()
        h2o.beta_features = True # to get the browser page special tab
        if (localhost):
            h2o.build_cloud(node_count=1, base_port=54327)
        else:
            h2o_hosts.build_cloud_with_hosts(node_count=1)
        h2o.beta_features = False
        # h2b.browseTheCloud()

    @classmethod
    def tearDownClass(cls):
        # h2o.sleep(3600)
        h2o.tear_down_cloud()

    def test_summary2_unifiles2(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        # new with 1000 bins. copy expected from R
        tryList = [
            # colname, (min, 25th, 50th, 75th, max)
            ('covtype.data', 'x.hex', [ ('C1', None, None, None, None, None)], 'home-0xdiag-datasets', 'standard'),
            ('runif_.csv', 'x.hex', [ ('C1', None, None, None, None, None)], '.', None),
            

        ]

        timeoutSecs = 10
        trial = 1
        n = h2o.nodes[0]
        lenNodes = len(h2o.nodes)

        x = 0
        timeoutSecs = 60
        for (csvFilename, hex_key, expectedCols, bucket, pathPrefix) in tryList:
            h2o.beta_features = False

            if pathPrefix:
                csvPathname = pathPrefix + "/" + csvFilename
            else:
                csvPathname = csvFilename

            csvPathnameFull = h2i.find_folder_and_filename(bucket, csvPathname, returnFullPath=True)
            parseResult = h2i.import_parse(bucket=bucket, path=csvPathname, 
                schema='put', hex_key=hex_key, timeoutSecs=10, doSummary=False)

            print csvFilename, 'parse time:', parseResult['response']['time']
            print "Parse result['destination_key']:", parseResult['destination_key']

            # We should be able to see the parse result?
            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
            print "\n" + csvFilename

            numRows = inspect["num_rows"]
            numCols = inspect["num_cols"]

            h2o.beta_features = True
            # okay to get more cols than we want
            summaryResult = h2o_cmd.runSummary(key=hex_key, max_qbins=MAX_QBINS)
            h2o.verboseprint("summaryResult:", h2o.dump_json(summaryResult))

            summaries = summaryResult['summaries']

            scipyCol = 0
            for expected, column in zip(expectedCols, summaries):
                colname = column['colname']
                if expected:
                    self.assertEqual(colname, expected[0])

                quantile = 0.5 if DO_MEDIAN else .999
                q = h2o.nodes[0].quantiles(source_key=hex_key, column=column['colname'],
                    quantile=quantile, max_qbins=MAX_QBINS, multiple_pass=1)
                qresult = q['result']
                qresult_multi = q['result_multi']
                h2p.blue_print("h2o quantiles result:", qresult)
                h2p.blue_print("h2o quantiles result_multi:", qresult_multi)
                h2p.blue_print("h2o quantiles iterations:", q['iterations'])
                h2p.blue_print("h2o quantiles interpolated:", q['interpolated'])
                print h2o.dump_json(q)

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

                    print "colname:", colname, "mean (2 places):", twoDecimals(mean)
                    print "colname:", colname, "std dev. (2 places):", twoDecimals(sd)

                    pct = stats['pct']
                    print "pct:", pct
                    print ""

                    # the thresholds h2o used, should match what we expected
                    expectedPct= [0.01, 0.05, 0.1, 0.25, 0.33, 0.5, 0.66, 0.75, 0.9, 0.95, 0.99]
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
                    pt = twoDecimals(pctile)
                    print "colname:", colname, "pctile (2 places):", pt
                    mx = twoDecimals(maxs)
                    mn = twoDecimals(mins)
                    print "colname:", colname, "maxs: (2 places):", mx
                    print "colname:", colname, "mins: (2 places):", mn

                    # FIX! we should do an exec and compare using the exec quantile too
                    actual = mn[0], pt[3], pt[5], pt[7], mx[0]
                    print "min/25/50/75/max colname:", colname, "(2 places):", actual
                    print "maxs colname:", colname, "(2 places):", mx
                    print "mins colname:", colname, "(2 places):", mn

                    ## ignore for blank colnames, issues with quoted numbers
                    # covtype is too big to do in scipy
                    if DO_TRY_SCIPY and colname!='' and (csvFilename != 'covtype.data'):
                        print "Going to try with scipy"
                        # don't do for enums
                        # also get the median with a sort (h2o_summ.percentileOnSortedlist()
                        print scipyCol, pctile[10]
                        generate_scipy_comparison(csvPathnameFull, col=scipyCol,
                            # h2oMedian=pctile[5 if DO_MEDIAN else 10], result_multi)
                            h2oMedian=qresult, h2oMedian2=qresult_multi, csvFilename=csvFilename)

                scipyCol += 1

            trial += 1



if __name__ == '__main__':
    h2o.unit_main()

