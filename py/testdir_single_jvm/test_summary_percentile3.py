import unittest, time, sys, random, math
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_import as h2i

print "Synthetic dataset with small/close reals and known mean and std dev"
print "Use small numbers"

DO_SCIPY_COMPARE = True
def generate_scipy_comparison(csvPathname):
    # this is some hack code for reading the csv and doing some percentile stuff in scipy
    from numpy import loadtxt, genfromtxt, savetxt

    dataset = loadtxt(
        open(csvPathname, 'r'),
        delimiter=',',
        dtype='float64');

    print "csv read for training, done"

    # we're going to strip just the last column for percentile work
    # used below
    NUMCLASSES = 10
    print "csv read for training, done"

    # data is last column
    # drop the output
    print dataset.shape
    from scipy import histogram
    import numpy
    if 1==1:
        print "histogram of dataset"
        print histogram(dataset,bins=NUMCLASSES)
        print numpy.mean(dataset, axis=0, dtype=numpy.float64)
        print numpy.std(dataset, axis=0, dtype=numpy.float64, ddof=0)
        print numpy.std(dataset, axis=0, dtype=numpy.float64, ddof=1)

    from scipy import stats
    # stats.scoreatpercentile(dataset, [10,20,30,40,50,60,70,80,90])

def write_syn_dataset(csvPathname, rowCount, colCount, expectedMin, expectedMax, SEED):
    r1 = random.Random(SEED)
    dsf = open(csvPathname, "w+")

    expectedRange = expectedMax - expectedMin
    expectedMean = expectedMin + (expectedRange/2)
    expectedSigma = .0001 * expectedRange

    # keep track of what ranges the noise hits..this effects the min/max h2o will see
    noiseMin = expectedMin
    noiseMax = expectedMax
    for i in range(rowCount):
        rowData = []

        # random noise that is a huge outlier
        if r1.randint(0,10000)==0:
            # ri = float(random.randint(-sys.maxint, sys.maxint))
            ri = 0
            if ri < noiseMin:
                noiseMin = ri
            if ri > noiseMax:
                noiseMax = ri
        else:
            ri = r1.normalvariate(expectedMean, expectedSigma)

        for j in range(colCount):
            # ri = r1.randint(expectedMin, expectedMax)
            rowData.append(ri)

        rowDataCsv = ",".join(map(str,rowData))
        dsf.write(rowDataCsv + "\n")

    dsf.close()
    return (expectedMean, expectedSigma, noiseMin, noiseMax)

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

    def test_summary(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        tryList = [
            (500000, 1, 'cD', 300, 0, 9), # expectedMin/Max must cause 10 values
            (500000, 2, 'cE', 300, 1, 10), # expectedMin/Max must cause 10 values
            (500000, 2, 'cF', 300, 2, 11), # expectedMin/Max must cause 10 values
        ]

        timeoutSecs = 10
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
        
            (expectedMean, expectedSigma, noiseMin, noiseMax) = write_syn_dataset(
                csvPathname, rowCount, colCount, expectedMin, expectedMax, SEEDPERFILE)


            parseResult = h2i.import_parse(path=csvPathname, schema='put', hex_key=hex_key, timeoutSecs=10, doSummary=False)
            print csvFilename, 'parse time:', parseResult['response']['time']
            print "Parse result['destination_key']:", parseResult['destination_key']

            # We should be able to see the parse result?
            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
            print "\n" + csvFilename

            summaryResult = h2o_cmd.runSummary(key=hex_key)
            h2o_cmd.infoFromSummary(summaryResult, noPrint=False)
            # remove bin_names because it's too big (256?) and bins
            # just touch all the stuff returned
            summary = summaryResult['summary']
            columnsList = summary['columns']
            for columns in columnsList:
                N = columns['N']
                self.assertEqual(N, rowCount)

                name = columns['name']
                stype = columns['type']
                self.assertEqual(stype, 'number')

                histogram = columns['histogram']
                bin_size = histogram['bin_size']
                ### self.assertEqual(bin_size, 1)

                bin_names = histogram['bin_names']
                bins = histogram['bins']
                nbins = histogram['bins']

                ## for b in bins:
                ##    e = .1 * rowCount
                ##    self.assertAlmostEqual(b, .1 * rowCount, delta=.01*rowCount, msg="Bins not right. b: %s e: %s" % (b, e))

                # not done if enum
                if stype != "enum":
                    smax = columns['max']
                    smin = columns['min']
                    percentiles = columns['percentiles']
                    thresholds = percentiles['thresholds']
                    values = percentiles['values']
                    mean = columns['mean']
                    sigma = columns['sigma']

                    expectedMaxBoth = max(expectedMax, noiseMax+1)
                    expectedMinBoth = min(expectedMin, noiseMin-1)

                    ### self.assertGreaterEqual(expectedMaxBoth, smax[0])
                    ### self.assertGreaterEqual(expectedMaxBoth, smax[1])
                    ### self.assertGreaterEqual(expectedMaxBoth, smax[2])
                    ### self.assertGreaterEqual(expectedMaxBoth, smax[3])
                    ### self.assertGreaterEqual(expectedMaxBoth, smax[4])
                    
                    ### self.assertLessEqual(expectedMinBoth, smin[0])
                    ### self.assertLessEqual(expectedMinBoth, smin[1])
                    ### self.assertLessEqual(expectedMinBoth, smin[2])
                    ### self.assertLessEqual(expectedMinBoth, smin[3])
                    ### self.assertLessEqual(expectedMinBoth, smin[4])

                    # apparently our 'percentile estimate" uses interpolation, so this check is not met by h2o
                    for v in values:
                        ##    self.assertIn(v,legalValues,"Value in percentile 'values' is not present in the dataset") 
                        # but: you would think it should be within the min-max range?
                        self.assertTrue(v >= expectedMinBoth,
                            "Percentile value %s should all be >= the min dataset value %s or noise min %s" % (v, expectedMin, noiseMin))
                        self.assertTrue(v <= expectedMaxBoth,
                            "Percentile value %s should all be <= the max dataset value %s or noise max %s" % (v, expectedMax, noiseMax))
                
                    if DO_SCIPY_COMPARE:
                        generate_scipy_comparison(csvPathname)
                    print "col name:", name, "mean:", mean, "expectedMean:", expectedMean
                    self.assertAlmostEqual(mean, expectedMean, delta=0.001)
                    # FIX! how do we estimate this
                    print "col name:", name, "sigma:", sigma, "expectedSigma:", expectedSigma
                    self.assertAlmostEqual(sigma, expectedSigma, delta=0.001)
                    
                    # if thresholds   = [0.01, 0.05, 0.1, 0.25, 0.33, 0.5, 0.66, 0.75, 0.9, 0.95, 0.99]
                    # values = [   0,    0,   1,    2,    3,   5,    7,    7,   9,    9,    10]

                    ## for t,v,e in zip(thresholds, values, eV):
                    ##     m = "Percentile threshold: %s with value %s should ~= %s" % (t, v, e)
                    ##     self.assertAlmostEqual(v, e, delta=0.5, msg=m)


if __name__ == '__main__':
    h2o.unit_main()

