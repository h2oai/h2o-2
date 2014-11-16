import unittest
import random, sys, time, os, math
sys.path.extend(['.','..','../..','py'])

import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i

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
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init(2,java_heap_GB=5)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_parse_bounds_csv_fvec(self):
        print "Random 0/1 for col1. Last has max col = 1, All have zeros for class."
        # h2b.browseTheCloud()
        SYNDATASETS_DIR = h2o.make_syn_dir()
        tryList = [
            (1000, 50, 'cC', 300),
            (1000, 999, 'cC', 300),
            (1000, 1000, 'cA', 300),
            # (1000, 100000, 'cB', 300),
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
                inspect = h2o_cmd.runInspect(None, parseResult['destination_key'], timeoutSecs=timeoutSecs)
                numCols = inspect['numCols']
                numRows = inspect['numRows']
                print "\n" + csvPathname, \
                    "    numRows:", "{:,}".format(numRows), \
                    "    numCols:", "{:,}".format(numCols)

                iCols = inspect['cols']
                iStats = []
                for stats in iCols:
                    iName = stats['name']
                    # just touching to make sure they are there
                    iNaCnt = stats['naCnt']
                    iMin = float(stats['min'])
                    iMax = float(stats['max'])
                    iMean = float(stats['mean'])
                    iStats.append( 
                        {
                            'name': iName,
                            'naCnt': iNaCnt, 
                            'min': iMin, 
                            'max': iMax, 
                            'mean': iMean, 
                        })

                # SUMMARY********************************
                summaryResult = h2o_cmd.runSummary(key=hex_key, max_ncols=colCount, timeoutSecs=timeoutSecs)
                h2o_cmd.infoFromSummary(summaryResult, noPrint=True)

                self.assertEqual(rowCount, numRows, 
                    msg="generated %s rows, parsed to %s rows" % (rowCount, numRows))

                columnsList = summaryResult['summaries']
                self.assertEqual(colCount, len(columnsList), 
                    msg="generated %s cols (including output).  summary has %s columns" % (colCount, len(columnsList)))

                c = 0
                for column in columnsList:
                    # get info from the inspect col for comparison
                    iMin = iStats[c]['min']
                    iMax = iStats[c]['max']
                    iMean = iStats[c]['mean']
                    iNaCnt = iStats[c]['naCnt']
                    c += 1

                    colname = column['colname']
                    stats = column['stats']
                    stype = column['type']
                    hstep = column['hstep']
                    hbrk = column['hstep']
                    hstart = column['hstart']

                    smax = stats['maxs']
                    smin = stats['mins']
                    sd = stats['sd']
                    smean = stats['mean']
                    # no zeroes if enum, but we're not enum here
                    zeros = stats['zeros']

                    self.assertEqual(iMin, smin[0], "inspect min %s != summary min %s" % (iMin, smin))
                    self.assertEqual(iMax, smax[0], "inspect max %s != summary max %s" % (iMax, smax))
                    self.assertEqual(iMean, smean, "inspect mean %s != summary mean %s" % (iMean, smean))
                    # no comparison for 'zeros'

                    # now, also compare expected values
                    if colname == "V1":
                        synNa = 0
                        # can reverse-engineer the # of zeroes, since data is always 1
                        synSum = synSumList[1] # could get the same sum for all ccols
                        synZeros = numRows - synSum
                        synSigma = 0.50
                        synMean = (synSum + 0.0)/numRows
                        synMin = [0.0, 1.0]
                        synMax = [1.0, 0.0]

                    elif colname == "V2":
                        synSum = 0
                        synSigma = 0
                        synMean = 0
                        if DO_NAN:
                            synZeros = 0
                            synNa = numRows
                            synMin = []
                            synMax = []
                        else:
                            synZeros = numRows
                            synNa = 0
                            synMin = [0.0]
                            synMax = [0.0]

                    # a single 1 in the last col
                    elif colname == "V" + str(colCount-1): # h2o puts a "V" prefix
                        synNa = 0
                        synSum = synSumList[colCount-1] 
                        synZeros = numRows - 1
                        # stddev.p 
                        # http://office.microsoft.com/en-us/excel-help/stdev-p-function-HP010335772.aspx

                        synMean = 1.0/numRows # why does this need to be a 1 entry list
                        synSigma = math.sqrt(pow((synMean - 1),2)/numRows)
                        print "last col with single 1. synSigma:", synSigma
                        synMin = [0.0, 1.0]
                        synMax = [1.0, 0.0]

                    else:
                        synNa = 0
                        synSum = 0
                        synZeros = numRows
                        synSigma = 0.0
                        synMean = 0.0
                        synMin = [0.0]
                        synMax = [0.0]

                    if DO_MEAN:
                        self.assertAlmostEqual(float(smean), synMean, places=6,
                            msg='col %s mean %s is not equal to generated mean %s' % (colname, smean, synMean))

                    # why are min/max one-entry lists in summary result. Oh..it puts N min, N max
                    self.assertTrue(smin >= synMin,
                        msg='col %s min %s is not >= generated min %s' % (colname, smin, synMin))

                    self.assertTrue(smax <= synMax,
                        msg='col %s max %s is not <= generated max %s' % (colname, smax, synMax))

                    # reverse engineered the number of zeroes, knowing data was always 1 if present?
                    if colname == "V65536" or colname == "V65537":
                        print "columns around possible zeros mismatch:", h2o.dump_json(columns)

                    self.assertEqual(zeros, synZeros,
                        msg='col %s zeros %s is not equal to generated zeros %s' % (colname, zeros, synZeros))

if __name__ == '__main__':
    h2o.unit_main()
