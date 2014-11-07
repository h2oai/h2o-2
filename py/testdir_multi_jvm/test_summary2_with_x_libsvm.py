import unittest
import random, sys, time, os
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i

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
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init(2,java_heap_GB=5)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_summary_with_x_libsvm (self):
        print "Empty rows except for the last, with all zeros for class. Single col at max"
        h2b.browseTheCloud()
        SYNDATASETS_DIR = h2o.make_syn_dir()
        tryList = [
            (100, 100, 'cA', 300),
            (100000, 100, 'cB', 300),
            (100, 1000, 'cC', 300),
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
                numCols = inspect['numCols']
                numRows = inspect['numRows']

                self.assertEqual(colNumberMax+1, numCols, 
                    msg="generated %s cols (including output).  parsed to %s cols" % (colNumberMax+1, numCols))
                self.assertEqual(rowCount, numRows, 
                    msg="generated %s rows, parsed to %s rows" % (rowCount, numRows))

                for x in range(numCols):
                    print "Doing summary with x=%s" % x
                    summaryResult = h2o_cmd.runSummary(key=hex_key, cols=x, timeoutSecs=timeoutSecs)
                    # skip the infoFromSummary check

                    colName = "C" + str(x+1)
                    print "Doing summary with col name x=%s" % colName
                    summaryResult = h2o_cmd.runSummary(key=hex_key, cols=colName, timeoutSecs=timeoutSecs)

                # do a final one with all columns for the current check below
                # FIX! we should update the check to check each individual summary result
                print "Doing and checking summary with no x=%s" % x
                summaryResult = h2o_cmd.runSummary(key=hex_key, max_ncols=colNumberMax+1, timeoutSecs=timeoutSecs)
                h2o_cmd.infoFromSummary(summaryResult, noPrint=True)

                # FIX! add some result checking


if __name__ == '__main__':
    h2o.unit_main()
