import unittest, time, sys, random
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_import as h2i, h2o_summ

DO_MEDIAN = True

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
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_summary2_percentile(self):
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
            legalValues = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10} # set. http://docs.python.org/2/library/stdtypes.html#set
            expectedMin = min(legalValues)
            expectedMax = max(legalValues)
            expectedUnique = (expectedMax - expectedMin) + 1
            mode = 0.5 # rounding to nearest int will shift us from this for expected mean
            expectedMean = 0.5
            expectedSigma = 0.5
            write_syn_dataset(csvPathname, rowCount, colCount, 
                low=expectedMin, high=expectedMax, mode=mode,
                SEED=SEEDPERFILE)

            csvPathnameFull = h2i.find_folder_and_filename('.', csvPathname, returnFullPath=True)
            parseResult = h2i.import_parse(path=csvPathname, schema='put', hex_key=hex_key, 
                timeoutSecs=10, doSummary=False)
            print "Parse result['destination_key']:", parseResult['destination_key']

            # We should be able to see the parse result?
            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
            print "\n" + csvFilename

            summaryResult = h2o_cmd.runSummary(key=hex_key)
            if h2o.verbose:
                print "summaryResult:", h2o.dump_json(summaryResult)

            summaries = summaryResult['summaries']
            scipyCol = 0
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

                for b in hbrk:
                    self.assertIn(int(b), legalValues)
                self.assertEqual(len(hbrk), len(legalValues))

                # self.assertAlmostEqual(hcnt[0], 0.5 * rowCount, delta=.01*rowCount)
                # self.assertAlmostEqual(hcnt[1], 0.5 * rowCount, delta=.01*rowCount)

                print "pctile:", pctile
                print "maxs:", maxs
                # we round to int, so we may introduce up to 0.5 rounding error? compared to "mode" target
                self.assertAlmostEqual(maxs[0], expectedMax, delta=0.01)
                print "mins:", mins
                self.assertAlmostEqual(mins[0], expectedMin, delta=0.01)

                for v in pctile:
                    self.assertTrue(v >= expectedMin,
                        "Percentile value %s should all be >= the min dataset value %s" % (v, expectedMin))
                    self.assertTrue(v <= expectedMax,
                        "Percentile value %s should all be <= the max dataset value %s" % (v, expectedMax))

                eV1 = [1.0, 1.0, 1.0, 3.0, 4.0, 5.0, 7.0, 8.0, 9.0, 10.0, 10.0]
                if expectedMin==1:
                    eV = eV1
                elif expectedMin==0:
                    eV = [e-1 for e in eV1]
                elif expectedMin==2:
                    eV = [e+1 for e in eV1]
                else:
                    raise Exception("Test doesn't have the expected percentileValues for expectedMin: %s" % expectedMin)

                if colname!='':
                    # don't do for enums
                    # also get the median with a sort (h2o_summ.percentileOnSortedlist()
                    h2o_summ.quantile_comparisons(
                        csvPathnameFull,
                        skipHeader=True,
                        col=scipyCol,
                        datatype='float',
                        quantile=0.5 if DO_MEDIAN else 0.999,
                        h2oSummary2=pctile[5 if DO_MEDIAN else 10],
                        # h2oQuantilesApprox=qresult_single,
                        # h2oQuantilesExact=qresult,
                        )

                scipyCol += 1


if __name__ == '__main__':
    h2o.unit_main()
