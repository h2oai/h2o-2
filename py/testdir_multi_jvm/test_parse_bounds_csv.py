import unittest
import random, sys, time, os
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import as h2i

DO_MEAN = False
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
            elif j==2:
                ri = None # na in col 2
            else:
                # single 1 in last col of last row
                # if i==(rowCount-1) and j==(rowCount-1):
                #     ri = 1
                # else:
                #     ri = 0
                ri = 0

            if ri is None:
                ri = ''
            else:
                synSumList[j] += ri
                ri = str(ri)
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
        ## h2b.browseTheCloud()
        SYNDATASETS_DIR = h2o.make_syn_dir()
        tryList = [
            (100000, 100, 'cB', 300),
            (100, 100, 'cA', 300),
            (100, 999, 'cC', 300),
            ]

        # h2b.browseTheCloud()
        for (rowCount, colCount, key2, timeoutSecs) in tryList:
                SEEDPERFILE = random.randint(0, sys.maxint)
                csvFilename = "syn_%s_%s_%s.csv" % (SEEDPERFILE, rowCount, colCount)
                csvPathname = SYNDATASETS_DIR + '/' + csvFilename

                print "Creating random", csvPathname
                # dict of col sums for comparison to exec col sums below
                synSumList = write_syn_dataset(csvPathname, rowCount, colCount, SEEDPERFILE)

                parseKey = h2o_cmd.parseFile(None, csvPathname, key2=key2, timeoutSecs=timeoutSecs, doSummary=False)
                print "Parse result['destination_key']:", parseKey['destination_key']
                inspect = h2o_cmd.runInspect(None, parseKey['destination_key'], timeoutSecs=timeoutSecs)
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

                summaryResult = h2o_cmd.runSummary(key=key2, timeoutSecs=timeoutSecs)
                h2o_cmd.infoFromSummary(summaryResult, noPrint=True)

                self.assertEqual(rowCount, num_rows, 
                    msg="generated %s rows, parsed to %s rows" % (rowCount, num_rows))

                summary = summaryResult['summary']
                columnsList = summary['columns']
                self.assertEqual(colCount, len(columnsList), 
                    msg="generated %s cols (including output).  summary has %s columns" % (colCount, len(columnsList)))

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

                    # definitely not type enum
                    print h2o.dump_json(columns)
                    # no zeroes if enum
                    zeros = columns['zeros']
                    na = columns['na']
                    smax = columns['max']
                    smin = columns['min']
                    mean = columns['mean']
                    sigma = columns['sigma']


                    # a single 1 in the last col
                    if name == "V" + str(colCount): # h2o puts a "V" prefix
                        synNa = 0
                        synSum = synSumList[colCount] 
                        synZeros = num_rows - 1
                        synSigma = None # not sure..depends on the # rows somehow (0 count vs 1 count)
                        synMean = 1.0/num_rows # why does this need to be a 1 entry list
                        synMin = [0.0, 1.0]
                        synMax = [1.0, 0.0]
                    elif name == ("V1"):
                        synNa = 0
                        # can reverse-engineer the # of zeroes, since data is always 1
                        synSum = synSumList[1] # could get the same sum for all ccols
                        synZeros = num_rows - synSum
                        synSigma = 0.50
                        synMean = (synSum + 0.0)/num_rows
                        synMin = [0.0, 1.0]
                        synMax = [1.0, 0.0]
                    elif name == ("V2"):
                        synNa = num_rows
                        # can reverse-engineer the # of zeroes, since data is always 1
                        synSum = 0
                        synZeros = 0
                        synSigma = 0
                        synMean = 0
                        synMin = []
                        synMax = []
                    else:
                        synNa = 0
                        synSum = 0
                        synZeros = num_rows
                        synSigma = 0.0
                        synMean = 0.0
                        synMin = [0.0]
                        synMax = [0.0]

                    if DO_MEAN:
                        self.assertAlmostEqual(float(mean), synMean, places=6,
                            msg='col %s mean %s is not equal to generated mean %s' % (name, mean, 0))

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
                        msg='col %s zeros %s is not equal to generated zeros count %s' % (name, zeros, synZeros))

                    self.assertEqual(stype, 'number',
                        msg='col %s type %s is not equal to %s' % (name, stype, 'number'))

                    # our random generation will have some variance for col 1. so just check to 2 places
                    if synSigma:
                        self.assertAlmostEqual(float(sigma), synSigma, delta=0.03,
                            msg='col %s sigma %s is not equal to generated sigma %s' % (name, sigma, synSigma))


if __name__ == '__main__':
    h2o.unit_main()
