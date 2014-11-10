import unittest, time, sys, random, math, getpass
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_import as h2i
import h2o_summ


DO_MEDIAN = True

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
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init()

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
            csvPathnameFull = h2i.find_folder_and_filename(None, csvPathname, returnFullPath=True)
            parseResult = h2i.import_parse(path=csvPathname, schema='put', hex_key=hex_key, timeoutSecs=30, doSummary=False)
            print "Parse result['destination_key']:", parseResult['destination_key']

            # We should be able to see the parse result?
            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
            print "\n" + csvFilename

            summaryResult = h2o_cmd.runSummary(key=hex_key, cols=0, max_ncols=1)
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

                for b in hcnt:
                    e = .1 * rowCount
                    self.assertAlmostEqual(b, .1 * rowCount, delta=.01*rowCount, 
                        msg="Bins not right. b: %s e: %s" % (b, e))

                print "pctile:", pctile
                print "maxs:", maxs
                self.assertEqual(maxs[0], expectedMax)
                print "mins:", mins
                self.assertEqual(mins[0], expectedMin)

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

            trial += 1

            # if colname!='' and expected[scipyCol]:
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

