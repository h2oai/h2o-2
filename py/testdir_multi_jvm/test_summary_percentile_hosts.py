import unittest, time, sys, random
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_import as h2i

def randint_triangular(low, high, mode): # inclusive bounds
    t = random.triangular(low, high, mode)
    # round to nearest int. Assume > 0
    if t > 0:
        return int(t + 0.5)
    else:
        return int(t - 0.5)

def write_syn_dataset(csvPathname, rowCount, colCount, low, high, mode, SEED):
    r1 = random.Random(SEED)
    dsf = open(csvPathname, "w+")
    for i in range(rowCount):
        rowData = [randint_triangular(low, high, mode) for j in range(colCount)]
        rowDataCsv = ",".join(map(str, rowData))
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
            h2o.build_cloud(3)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_summary(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        tryList = [
            (100000, 1, 'cD', 300),
            (100000, 2, 'cE', 300),
        ]

        timeoutSecs = 10
        trial = 1
        for (rowCount, colCount, hex_key, timeoutSecs) in tryList:
            print 'Trial:', trial
            SEEDPERFILE = random.randint(0, sys.maxint)
            csvFilename = 'syn_' + "binary" + "_" + str(rowCount) + 'x' + str(colCount) + '.csv'
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename

            print "Creating random", csvPathname
            legalValues = {1, 2, 3, 4} # set. http://docs.python.org/2/library/stdtypes.html#set
            expectedMin = min(legalValues)
            expectedMax = max(legalValues)
            expectedUnique = (expectedMax - expectedMin) + 1
            mode = 3 # rounding to nearest int will shift us from this for expected mean
            expectedMean = 2.66
            expectedSigma = 0.68
            write_syn_dataset(csvPathname, rowCount, colCount, 
                low=expectedMin, high=expectedMax, mode=mode,
                SEED=SEEDPERFILE)

            parseResult = h2i.import_parse(path=csvPathname, hex_key=hex_key, schema='put', timeoutSecs=10, doSummary=False)
            print csvFilename, 'parse time:', parseResult['response']['time']
            print "Parse result['destination_key']:", parseResult['destination_key']

            # We should be able to see the parse result?
            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
            print "\n" + csvFilename

            summaryResult = h2o_cmd.runSummary(key=hex_key)
            # just touch all the stuff returned
            h2o_cmd.infoFromSummary(summaryResult, noPrint=False)
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
                self.assertEqual(bin_size, 1)

                bin_names = histogram['bin_names']
                for b in bin_names:
                    self.assertIn(int(b), legalValues)

                bins = histogram['bins']
                nbins = histogram['bins']
                self.assertEqual(len(bins), len(legalValues))
                # this distribution assumes 4 values with mean on the 3rd
                self.assertAlmostEqual(bins[0], 0.042 * rowCount, delta=.01*rowCount, msg="histogram bins 0")
                self.assertAlmostEqual(bins[1], 0.333 * rowCount, delta=.01*rowCount, msg="histogram bins 1")
                self.assertAlmostEqual(bins[2], 0.541 * rowCount, delta=.01*rowCount, msg="histogram bins 2")
                self.assertAlmostEqual(bins[3], 0.083 * rowCount, delta=.01*rowCount, msg="histogram bins 3")

                # not done if enum
                if stype != "enum":
                    smax = columns['max']
                    smin = columns['min']
                    percentiles = columns['percentiles']
                    thresholds = percentiles['thresholds']
                    values = percentiles['values']
                    mean = columns['mean']
                    sigma = columns['sigma']

                    self.assertEqual(smax[0], expectedMax)
                    self.assertEqual(smin[0], expectedMin)
                    # apparently our 'percentile estimate" uses interpolation, so this check is not met by h2o
                    for v in values:
                        ##    self.assertIn(v,legalValues,"Value in percentile 'values' is not present in the dataset") 
                        # but: you would think it should be within the min-max range?
                        self.assertTrue(v >= expectedMin, 
                            "Percentile value %s should all be >= the min dataset value %s" % (v, expectedMin))
                        self.assertTrue(v <= expectedMax, 
                            "Percentile value %s should all be <= the max dataset value %s" % (v, expectedMax))
                
                    # we round to int, so we may introduce up to 0.5 rounding error? compared to "mode" target
                    self.assertAlmostEqual(mean, expectedMean, delta=0.02, msg="mean")
                    self.assertAlmostEqual(sigma, expectedSigma, delta=0.02, msg="sigma")

            trial += 1


if __name__ == '__main__':
    h2o.unit_main()
