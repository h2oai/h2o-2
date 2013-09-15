import unittest
import random, sys, time, os
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import as h2i

def write_syn_dataset(csvPathname, rowCount, colCount, SEEDPERFILE):
    # we can do all sorts of methods off the r object
    r = random.Random(SEEDPERFILE)

    def addValToRowStuff(colNumber, val, rowData, synColSumDict):
        if val!=0:
            rowData.append(str(colNumber) + ":" + str(val)) # f should always return string
            if colNumber in synColSumDict:
                synColSumDict[colNumber] += val # sum of column (dict)
            else:
                synColSumDict[colNumber] = val # sum of column (dict)

    dsf = open(csvPathname, "w+")

    synColSumDict = {0: 0} # guaranteed to have col 0 for output
    for i in range(rowCount):
        rowData = []
        if i==(rowCount-1): # last row
            val = 1
            colNumber = colCount # max
            colNumberMax = colNumber
            addValToRowStuff(colNumber, val, rowData, synColSumDict)

        # add output
        val = 0
        rowData.insert(0, val)
        synColSumDict[0] += val # sum of column (dict)

        rowDataCsv = " ".join(map(str,rowData))
        dsf.write(rowDataCsv + "\n")

    dsf.close()
    return (colNumberMax, synColSumDict)

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED, localhost
        SEED = h2o.setup_random_seed()
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(2,java_heap_GB=5)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_parse_bounds_libsvm (self):
        print "Empty rows except for the last, with all zeros for class. Single col at max"
        h2b.browseTheCloud()
        SYNDATASETS_DIR = h2o.make_syn_dir()
        tryList = [
            (100, 100, 'cA', 300),
            (100000, 100, 'cB', 300),
            (100, 10000, 'cC', 300),
            ]

        # h2b.browseTheCloud()
        for (rowCount, colCount, hex_key, timeoutSecs) in tryList:
                SEEDPERFILE = random.randint(0, sys.maxint)
                csvFilename = "syn_%s_%s_%s.csv" % (SEEDPERFILE, rowCount, colCount)
                csvPathname = SYNDATASETS_DIR + '/' + csvFilename

                print "Creating random", csvPathname
                # dict of col sums for comparison to exec col sums below
                (colNumberMax, synColSumDict) = write_syn_dataset(csvPathname, rowCount, colCount, SEEDPERFILE)
                parseResult = h2i.import_parse(path=csvPathname, schema='put', hex_key=hex_key, timeoutSecs=timeoutSecs, 
                    doSummary=False)
                print "Parse result['destination_key']:", parseResult['destination_key']
                inspect = h2o_cmd.runInspect(None, parseResult['destination_key'], max_column_display=colNumberMax+1, 
                    timeoutSecs=timeoutSecs)
                num_cols = inspect['num_cols']
                num_rows = inspect['num_rows']

                self.assertEqual(colNumberMax+1, num_cols, 
                    msg="generated %s cols (including output).  parsed to %s cols" % (colNumberMax+1, num_cols))
                self.assertEqual(rowCount, num_rows, 
                    msg="generated %s rows, parsed to %s rows" % (rowCount, num_rows))

                for x in range(num_cols):
                    print "Doing summary with x=%s" % x
                    summaryResult = h2o_cmd.runSummary(key=hex_key, x=x, timeoutSecs=timeoutSecs)
                    # skip the infoFromSummary check

                    if x==0:
                        colName = "Target"
                    else:
                        colName = "V" + str(x)
                    print "Doing summary with col name x=%s" % colName
                    summaryResult = h2o_cmd.runSummary(key=hex_key, x=x, timeoutSecs=timeoutSecs)

                # do a final one with all columns for the current check below
                # FIX! we should update the check to check each individual summary result
                print "Doing and checking summary with no x=%s" % x
                summaryResult = h2o_cmd.runSummary(key=hex_key, max_column_display=colNumberMax+1, timeoutSecs=timeoutSecs)
                h2o_cmd.infoFromSummary(summaryResult, noPrint=True)

                summary = summaryResult['summary']
                columnsList = summary['columns']
                self.assertEqual(colNumberMax+1, len(columnsList), 
                    msg="generated %s cols (including output).  summary has %s columns" % (colNumberMax+1, len(columnsList)))

                for columns in columnsList:
                    N = columns['N']
                    # self.assertEqual(N, rowCount)
                    name = columns['name']
                    stype = columns['type']

                    histogram = columns['histogram']
                    bin_size = histogram['bin_size']
                    bin_names = histogram['bin_names']
                    bins = histogram['bins']
                    nbins = histogram['bins']

                    # definitely not enums
                    zeros = columns['zeros']
                    na = columns['na']
                    smax = columns['max']
                    smin = columns['min']
                    mean = columns['mean']
                    sigma = columns['sigma']

                    # a single 1 in the last col
                    # print name
                    if name == ("V" + str(colNumberMax)): # h2o puts a "V" prefix
                        synMean = 1.0/num_rows # why does this need to be a 1 entry list
                        synMin = [0.0, 1.0]
                        synMax = [1.0, 0.0]
                    else:
                        synMean = 0.0
                        synMin = [0.0]
                        synMax = [0.0]

                    self.assertEqual(float(mean), synMean,
                        msg='col %s mean %s is not equal to generated mean %s' % (name, mean, 0))
                    # why are min/max one-entry lists in summary result. Oh..it puts N min, N max
                    self.assertEqual(smin, synMin,
                        msg='col %s min %s is not equal to generated min %s' % (name, smin, synMin))
                    self.assertEqual(smax, synMax,
                        msg='col %s max %s is not equal to generated max %s' % (name, smax, synMax))
                    self.assertEqual(0, na,
                        msg='col %s num_missing_values %d should be 0' % (name, na))


if __name__ == '__main__':
    h2o.unit_main()
