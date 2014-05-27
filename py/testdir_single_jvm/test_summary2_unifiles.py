import unittest, time, sys, random, math, getpass
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_import as h2i, h2o_util, h2o_browse as h2b, h2o_print as h2p
import h2o_summ

DO_TRY_SCIPY = False
if getpass.getuser()=='kevin' or getpass.getuser()=='jenkins':
    DO_TRY_SCIPY = True

DO_MEDIAN = True

# FIX!. we seem to lose accuracy with fewer bins -> more iterations. Maybe we're leaking or ??
# this test failed (if run as user kevin) with 10 bins
MAX_QBINS = 1000 # pass
MAX_QBINS = 100 # pass

# this one doesn't fail with 10 bins
# this failed. interestingly got same number as 1000 bin summary2 (the 7.433..
# on runifA.csv (2nd col?)
# MAX_QBINS = 20
# Exception: h2o quantile multipass is not approx. same as sort algo. h2o_util.assertApproxEqual failed comparing 7.43337413296 and 8.26268245. {'tol': 2e-07}.

MAX_QBINS = 20

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

    def test_summary2_unifiles(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()

        # new with 1000 bins. copy expected from R
        tryList = [
            ('cars.csv', 'c.hex', [
                (None, None,None,None,None,None),
                ('economy (mpg)', None,None,None,None,None),
                ('cylinders', None,None,None,None,None),
            ],
            ),
            ('runifA.csv', 'A.hex', [
                (None,  1.00, None, 50.00, 75.00, 100.0),
                ('x', -99.0, -44.7, 7.43, 58.00, 91.7),
            ],
            ),
            # colname, (min, 25th, 50th, 75th, max)
            ('runif.csv', 'x.hex', [
                (None,  1.00, 5000.0, 10000.0, 15000.0, 20000.00),
                ('D', -5000.00, -3735.0, -2443, -1187.0, 99.8),
                ('E', -100000.0, -49208.0, 1783.8, 50621.9, 100000.0),
                ('F', -1.00, -0.4886, 0.00868, 0.5048, 1.00),
            ],
            ),
            ('runifB.csv', 'B.hex', [
                (None,  1.00, 2501.00, 5001.00, 7501.00, 10000.00),
                ('x', -100.00, -50.0, 0.97, 51.7, 100,00),
            ],
            ),

            ('runifC.csv', 'C.hex', [
                (None,  1.00, 25002.00, 50002.00, 75002.00, 100000.00),
                ('x', -100.00, -50.45, -1.135, 49.28, 100.00),
            ],
            ),
        ]


        timeoutSecs = 10
        trial = 1
        n = h2o.nodes[0]
        lenNodes = len(h2o.nodes)

        x = 0
        timeoutSecs = 60
        for (csvFilename, hex_key, expectedCols) in tryList:
            h2o.beta_features = False

            csvPathname = csvFilename
            csvPathnameFull = h2i.find_folder_and_filename('smalldata', csvPathname, returnFullPath=True)
            parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname,
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
            # summaryResult = h2o_cmd.runSummary(key=hex_key, max_qbins=MAX_QBINS)
            print "keep summary2 with results for 1000 qbins, so it's accuracy doesn't degrade when fewer are used for 2/Quantile"
            summaryResult = h2o_cmd.runSummary(key=hex_key, max_qbins=1000)
            h2o.verboseprint("summaryResult:", h2o.dump_json(summaryResult))
            summaries = summaryResult['summaries']

            scipyCol = 0
            for expected, column in zip(expectedCols, summaries):
                colname = column['colname']
                if expected[0]:
                    self.assertEqual(colname, expected[0]), colname, expected[0]
                else:
                    # if the colname is None, skip it (so we don't barf on strings on the h2o quantile page
                    scipyCol += 1
                    continue

                quantile = 0.5 if DO_MEDIAN else .999
                # h2o has problem if a list of columns (or dictionary) is passed to 'column' param
                q = h2o.nodes[0].quantiles(source_key=hex_key, column=column['colname'],
                    quantile=quantile, max_qbins=MAX_QBINS, multiple_pass=2, interpolation_type=2) # mean
                qresult = q['result']
                qresult_single = q['result_single']
                h2p.blue_print("h2o quantiles result:", qresult)
                h2p.blue_print("h2o quantiles result_single:", qresult_single)
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

                    print "colname:", colname, "mean (2 places):", h2o_util.twoDecimals(mean)
                    print "colname:", colname, "std dev. (2 places):",  h2o_util.twoDecimals(sd)

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
                    pt = h2o_util.twoDecimals(pctile)
                    print "colname:", colname, "pctile (2 places):", pt
                    mx = h2o_util.twoDecimals(maxs)
                    mn = h2o_util.twoDecimals(mins)
                    print "colname:", colname, "maxs: (2 places):", mx
                    print "colname:", colname, "mins: (2 places):", mn

                    # FIX! we should do an exec and compare using the exec quantile too
                    actual = mn[0], pt[3], pt[5], pt[7], mx[0]
                    print "min/25/50/75/max colname:", colname, "(2 places):", actual
                    print "maxs colname:", colname, "(2 places):", mx
                    print "mins colname:", colname, "(2 places):", mn

                    # don't check if colname is empty..means it's a string and scipy doesn't parse right?
                    # need to ignore the car names
                    if colname!='' and expected[scipyCol]:
                        # don't do for enums
                        # also get the median with a sort (h2o_summ.percentileOnSortedlist()
                        h2o_summ.quantile_comparisons(
                            csvPathnameFull,
                            skipHeader=True,
                            col=scipyCol,
                            datatype='float',
                            quantile=0.5 if DO_MEDIAN else 0.999,
                            h2oSummary2=pctile[5 if DO_MEDIAN else 10],
                            h2oQuantilesApprox=qresult_single,
                            h2oQuantilesExact=qresult,
                            )


                scipyCol += 1

            trial += 1



if __name__ == '__main__':
    h2o.unit_main()

