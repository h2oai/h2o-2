import unittest, time, sys, random, math, getpass
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_import as h2i


DO_SCIPY_COMPARE = False

def generate_scipy_comparison(csvPathname):
    # this is some hack code for reading the csv and doing some percentile stuff in scipy
    from numpy import loadtxt, genfromtxt, savetxt

    dataset = loadtxt(
        open(csvPathname, 'r'),
        delimiter=',',
        dtype='int16');

    print "csv read for training, done"

    # we're going to strip just the last column for percentile work
    # used below
    NUMCLASSES = 10
    print "csv read for training, done"

    # data is last column
    # drop the output
    print dataset.shape
    if 1==1:
        n_features = len(dataset[0]) - 1;
        print "n_features:", n_features

        # get the end
        target = [x[-1] for x in dataset]

        print "histogram of target"
        from scipy import histogram
        print histogram(target, bins=NUMCLASSES)

        print target[0]
        print target[1]

    from scipy import stats
    thresholds   = [0.01, 0.05, 0.1, 0.25, 0.33, 0.5, 0.66, 0.75, 0.9, 0.95, 0.99]
    per = [100 * t for t in thresholds]
    print "scipy per:", per
    a = stats.scoreatpercentile(dataset, per=per)
    print "scipy percentiles:", a

def write_syn_dataset(csvPathname, rowCount, colCount, expectedMin, expectedMax, SEED):
    r1 = random.Random(SEED)
    dsf = open(csvPathname, "w+")

    expectedRange = (expectedMax - expectedMin) + 1
    for i in range(rowCount):
        rowData = []
        ri = expectedMin + (i % expectedRange)
        for j in range(colCount):
            # ri = r1.randint(expectedMin, expectedMax)
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

    def test_summary2_percentile2(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        tryList = [
            (500000, 2, 'cD', 300, 0, 9), # expectedMin/Max must cause 10 values
            (500000, 2, 'cE', 300, 1, 10), # expectedMin/Max must cause 10 values
            (500000, 2, 'cF', 300, 2, 11), # expectedMin/Max must cause 10 values
        ]

        timeoutSecs = 10
        trial = 1
        n = h2o.nodes[0]
        lenNodes = len(h2o.nodes)

        x = 0
        for (rowCount, colCount, hex_key, timeoutSecs, expectedMin, expectedMax) in tryList:
            SEEDPERFILE = random.randint(0, sys.maxint)
            x += 1

            csvFilename = 'syn_' + "binary" + "_" + str(rowCount) + 'x' + str(colCount) + '.csv'
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename

            print "Creating random", csvPathname
            legalValues = {}
            for x in range(expectedMin, expectedMax):
                legalValues[x] = x
        
            write_syn_dataset(csvPathname, rowCount, colCount, expectedMin, expectedMax, SEEDPERFILE)
            h2o.beta_features = False
            parseResult = h2i.import_parse(path=csvPathname, schema='put', hex_key=hex_key, timeoutSecs=10, doSummary=False)
            print csvFilename, 'parse time:', parseResult['response']['time']
            print "Parse result['destination_key']:", parseResult['destination_key']

            # We should be able to see the parse result?
            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
            print "\n" + csvFilename

            h2o.beta_features = True
            summaryResult = h2o_cmd.runSummary(key=hex_key, cols=0, max_ncols=1)
            if h2o.verbose:
                print "summaryResult:", h2o.dump_json(summaryResult)

            # pct: [0.01, 0.05, 0.1, 0.25, 0.33, 0.5, 0.66, 0.75, 0.9, 0.95, 0.99]
            # pctile: [1.0, 1.0, 1.0, 1.0, 1.0, 2.0, 2.0, 2.0, 3.0, 6.0, 7.0]
            # hstart: 1.0
            # hstep: 1.0
            # hbrk: [u'1', u'2', u'3', u'4', u'5', u'6', u'7']
            # hcnt: [4236800, 5666020, 715080, 54940, 189860, 347340, 410200]

            summaries = summaryResult['summaries']
            for column in summaries:
                colname = column['colname']
                coltype = column['type']
                nacnt = column['nacnt']

                stats = column['stats']
                stattype= stats['type']
                mean = stats['mean']
                sd = stats['sd']
                zeros = stats['zeros']
                mins = stats['mins']
                maxs = stats['maxs']
                pct = stats['pct']
                pctile = stats['pctile']

                hstart = column['hstart']
                hstep = column['hstep']
                hbrk = column['hbrk']
                hcnt = column['hcnt']

                for b in hcnt:
                    e = .1 * rowCount
                    self.assertAlmostEqual(b, .1 * rowCount, delta=.01*rowCount, 
                        msg="Bins not right. b: %s e: %s" % (b, e))

                print "pctile:", pctile
                print "maxs:", maxs
                self.assertEqual(maxs[0], expectedMax)
                # self.assertEqual(maxs[1], expectedMax-1)
                # self.assertEqual(maxs[2], expectedMax-2)
                # self.assertEqual(maxs[3], expectedMax-3)
                # self.assertEqual(maxs[4], expectedMax-4)
                
                print "mins:", mins
                self.assertEqual(mins[0], expectedMin)
                # self.assertEqual(mins[1], expectedMin+1)
                # self.assertEqual(mins[2], expectedMin+2)
                # self.assertEqual(mins[3], expectedMin+3)
                # self.assertEqual(mins[4], expectedMin+4)

                # apparently our 'percentile estimate" uses interpolation, so this check is not met by h2o
                for v in pctile:
                    ##    self.assertIn(v,legalValues,"Value in percentile 'percentileValues' is not present in the dataset") 
                    # but: you would think it should be within the min-max range?
                    self.assertTrue(v >= expectedMin, 
                        "Percentile value %s should all be >= the min dataset value %s" % (v, expectedMin))
                    self.assertTrue(v <= expectedMax, 
                        "Percentile value %s should all be <= the max dataset value %s" % (v, expectedMax))
            
                # self.assertAlmostEqual(mean, (expectedMax+expectedMin)/2.0, delta=0.1)
                # self.assertAlmostEqual(sigma, 2.9, delta=0.1)
                
                # since we distribute the outputs evenly from 0 to 9, we can check 
                # that the value is equal to the threshold (within some delta

                # is this right?
                # if thresholds   = [0.01, 0.05, 0.1, 0.25, 0.33, 0.5, 0.66, 0.75, 0.9, 0.95, 0.99]
                # percentileValues = [   0,    0,   1,    2,    3,   5,    7,    7,   9,    9,    10]
                eV1 = [1.0, 1.0, 1.0, 3.0, 4.0, 5.0, 7.0, 8.0, 9.0, 10.0, 10.0]
                if expectedMin==1:
                    eV = eV1
                elif expectedMin==0:
                    eV = [e-1 for e in eV1]
                elif expectedMin==2:
                    eV = [e+1 for e in eV1]
                else:
                    raise Exception("Test doesn't have the expected percentileValues for expectedMin: %s" % expectedMin)

                if 1==0:
                    for v,e in zip(pctile, eV):
                        m = "Percentile: value %s should ~= %s" % (v, e)
                        self.assertAlmostEqual(v, e, delta=0.5, msg=m)

            trial += 1

            if DO_SCIPY_COMPARE:
                generate_scipy_comparison(csvPathname)

if __name__ == '__main__':
    h2o.unit_main()

