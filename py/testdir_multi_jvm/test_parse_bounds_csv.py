import unittest
import random, sys, time, os, math
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import as h2i

DO_MEAN = False
DO_NAN = False
def write_syn_dataset(csvPathname, rowCount, colCount, SEED):
    r1 = random.Random(SEED)
    dsf = open(csvPathname, "w+")
    # write header
    rowData = ['Target']
    rowData.extend(['V' + str(i) for i in range(1, colCount)])
    rowDataCsv = ",".join(rowData)
    dsf.write(rowDataCsv + "\n")

    synSumList = [0 for i in range(colCount)]
    for i in range(rowCount):
        rowData = []
        for j in range(colCount):
            if j==1:
                ri = random.randint(0,1) # random in col 1
                synSumList[j] += ri
                ri = str(ri)
            elif j==2:
                ri = '' if DO_NAN else '0' # na in col 2
            elif j==(colCount-1) and i==(rowCount-1): # last row/col
                synSumList[j] += 1
                ri = '1'
            else:
                ri = '0'
            rowData.append(ri)

        rowDataCsv = ",".join(rowData)
        dsf.write(rowDataCsv + "\n")

    dsf.close()
    return synSumList

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

    def test_parse_bounds_csv (self):
        print "Random 0/1 for col1. Last has max col = 1, All have zeros for class."
        h2b.browseTheCloud()
        SYNDATASETS_DIR = h2o.make_syn_dir()
        tryList = [
            (1000, 100000, 'cB', 300),
            (1000, 1000, 'cA', 300),
            (1000, 999, 'cC', 300),
            ]

        # h2b.browseTheCloud()
        for (rowCount, colCount, hex_key, timeoutSecs) in tryList:
                SEEDPERFILE = random.randint(0, sys.maxint)
                csvFilename = "syn_%s_%s_%s.csv" % (SEEDPERFILE, rowCount, colCount)
                csvPathname = SYNDATASETS_DIR + '/' + csvFilename

                print "Creating random", csvPathname
                # dict of col sums for comparison to exec col sums below
                synSumList = write_syn_dataset(csvPathname, rowCount, colCount, SEEDPERFILE)

                # PARSE**********************
                parseResult = h2i.import_parse(path=csvPathname, hex_key=hex_key, schema='put', 
                    timeoutSecs=timeoutSecs, doSummary=False)
                print "Parse result['destination_key']:", parseResult['destination_key']

                # INSPECT*******************
                inspect = h2o_cmd.runInspect(None, parseResult['destination_key'], 
                    max_column_display=colCount, timeoutSecs=timeoutSecs)
                num_cols = inspect['num_cols']
                num_rows = inspect['num_rows']
                row_size = inspect['row_size']
                value_size_bytes = inspect['value_size_bytes']
                print "\n" + csvPathname, \
                    "    num_rows:", "{:,}".format(num_rows), \
                    "    num_cols:", "{:,}".format(num_cols), \
                    "    value_size_bytes:", "{:,}".format(value_size_bytes), \
                    "    row_size:", "{:,}".format(row_size)

                expectedRowSize = num_cols * 1 # plus output
                expectedValueSize = expectedRowSize * num_rows
                self.assertEqual(row_size, expectedRowSize,
                    msg='row_size %s is not expected num_cols * 1 byte: %s' % \
                    (row_size, expectedRowSize))
                self.assertEqual(value_size_bytes, expectedValueSize,
                    msg='value_size_bytes %s is not expected row_size * rows: %s' % \
                    (value_size_bytes, expectedValueSize))

                iCols = inspect['cols']
                iColNameToOffset = {}
                for iColDict in iCols:
                    # even though 'offset' exists, we'll use 'name' as the common key
                    # to compare inspect and summary results
                    iName = iColDict['name']
                    iOffset = iColDict['offset']
                    iColNameToOffset[iName] = iOffset
                    # just touching to make sure they are there
                    num_missing_values = iColDict['num_missing_values']
                    iMin = float(iColDict['min'])
                    iMax = float(iColDict['max'])
                    iMean = float(iColDict['mean'])
                    iVariance = float(iColDict['variance'])

                # SUMMARY********************************
                summaryResult = h2o_cmd.runSummary(key=hex_key, max_column_display=colCount, timeoutSecs=timeoutSecs)
                h2o_cmd.infoFromSummary(summaryResult, noPrint=True)

                self.assertEqual(rowCount, num_rows, 
                    msg="generated %s rows, parsed to %s rows" % (rowCount, num_rows))

                summary = summaryResult['summary']
                columnsList = summary['columns']
                self.assertEqual(colCount, len(columnsList), 
                    msg="generated %s cols (including output).  summary has %s columns" % (colCount, len(columnsList)))

                for columns in columnsList:
                    name = columns['name']
                    iOffset = iColNameToOffset[name]
                    iColDict = iCols[iOffset]

                    iMin = iColDict['min']
                    iMax = iColDict['max']
                    iMean = iColDict['mean']
                    iVariance = iColDict['variance']
                    iNumMissingValues = iColDict['num_missing_values']

                    
                    # from the summary
                    N = columns['N']
                    stype = columns['type']

                    histogram = columns['histogram']
                    bin_size = histogram['bin_size']
                    bin_names = histogram['bin_names']
                    bins = histogram['bins']
                    nbins = histogram['nbins']

                    smax = columns['max']
                    smin = columns['min']
                    smean = columns['mean']
                    sigma = columns['sigma']
                    na = columns['na']
                    # no zeroes if enum, but we're not enum here
                    zeros = columns['zeros']

                    self.assertEqual(iMin, smin[0], "inspect min %s != summary min %s" % (iMin, smin))
                    self.assertEqual(iMax, smax[0], "inspect max %s != summary max %s" % (iMax, smax))
                    self.assertEqual(iMean, smean, "inspect mean %s != summary mean %s" % (iMean, smean))
                    self.assertEqual(iVariance, sigma, "inspect variance %s != summary sigma %s" % (iVariance, sigma))
                    self.assertEqual(iNumMissingValues, na, "inspect num_missing_values %s != summary na %s" % (iNumMissingValues, na))
                    # no comparison for 'zeros'

                    # now, also compare expected values
                    if name == "V1":
                        synNa = 0
                        # can reverse-engineer the # of zeroes, since data is always 1
                        synSum = synSumList[1] # could get the same sum for all ccols
                        synZeros = num_rows - synSum
                        synSigma = 0.50
                        synMean = (synSum + 0.0)/num_rows
                        synMin = [0.0, 1.0]
                        synMax = [1.0, 0.0]

                    elif name == "V2":
                        synSum = 0
                        synSigma = 0
                        synMean = 0
                        if DO_NAN:
                            synZeros = 0
                            synNa = num_rows
                            synMin = []
                            synMax = []
                        else:
                            synZeros = num_rows
                            synNa = 0
                            synMin = [0.0]
                            synMax = [0.0]

                    # a single 1 in the last col
                    elif name == "V" + str(colCount-1): # h2o puts a "V" prefix
                        synNa = 0
                        synSum = synSumList[colCount-1] 
                        synZeros = num_rows - 1
                        # stddev.p 
                        # http://office.microsoft.com/en-us/excel-help/stdev-p-function-HP010335772.aspx

                        synMean = 1.0/num_rows # why does this need to be a 1 entry list
                        synSigma = math.sqrt(pow((synMean - 1),2)/num_rows)
                        print "last col with single 1. synSigma:", synSigma
                        synMin = [0.0, 1.0]
                        synMax = [1.0, 0.0]

                    else:
                        synNa = 0
                        synSum = 0
                        synZeros = num_rows
                        synSigma = 0.0
                        synMean = 0.0
                        synMin = [0.0]
                        synMax = [0.0]

                    if DO_MEAN:
                        self.assertAlmostEqual(float(smean), synMean, places=6,
                            msg='col %s mean %s is not equal to generated mean %s' % (name, smean, synMean))

                    # why are min/max one-entry lists in summary result. Oh..it puts N min, N max
                    self.assertTrue(smin >= synMin,
                        msg='col %s min %s is not >= generated min %s' % (name, smin, synMin))

                    self.assertTrue(smax <= synMax,
                        msg='col %s max %s is not <= generated max %s' % (name, smax, synMax))

                    # reverse engineered the number of zeroes, knowing data was always 1 if present?
                    if name == "V65536" or name == "V65537":
                        print "columns around possible zeros mismatch:", h2o.dump_json(columns)

                    self.assertEqual(na, synNa,
                        msg='col %s na %s is not equal to generated na %s' % (name, na, synNa))

                    self.assertEqual(zeros, synZeros,
                        msg='col %s zeros %s is not equal to generated zeros %s' % (name, zeros, synZeros))

                    self.assertEqual(stype, 'number',
                        msg='col %s type %s is not equal to %s' % (name, stype, 'number'))

                    # our random generation will have some variance for col 1. so just check to 2 places
                    if synSigma:
                        self.assertAlmostEqual(float(sigma), synSigma, delta=0.03,
                            msg='col %s sigma %s is not equal to generated sigma %s' % (name, sigma, synSigma))

if __name__ == '__main__':
    h2o.unit_main()
