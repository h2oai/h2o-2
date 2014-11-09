import unittest, random, time, sys, string
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i, h2o_browse as h2b

print "Not sure if we'll need quotes around these time formats"

def a0(epochTime): return time.asctime(epochTime)
def a1(epochTime): return time.strftime("%c", epochTime)
def a2(epochTime): return time.strftime("%x", epochTime)
def a3(epochTime): return time.strftime("%X", epochTime)
def a4(epochTime): return time.strftime("%X %x", epochTime)
def a5(epochTime): return time.strftime("%x %X", epochTime)

timeFormatFuncList = [
    a0, a1, a2, a3, a4, a5
]

def getRandomDate():
    epochSecs = random.randint(0,2000000000)
    epochTime = time.gmtime(epochSecs)
    a = random.randint(0,len(timeFormatFuncList)-1)
    # randomly pick one of the ways to format the random time
    b = timeFormatFuncList[a](epochTime)
    return b

def rand_rowData(colCount=6):
    a = [getRandomDate() for fields in range(colCount)]

    # thru a random NA in, on every row?
    naCol = random.randint(0,colCount-1)
    # FIX or maybe junk?
    a[naCol] = ''

    # do a random upper case of one
    upCol = random.randint(0,colCount-1)
    a[upCol] = a[upCol].upper()
    
    b = ",".join(map(str,a))
    print b
    return b

def rand_header(colCount=6):
    # string.printable string.punctuation string.whitespace
    choiceStr = string.ascii_uppercase + string.letters + string.digits + " "
    h = []
    for c in range(colCount):
        h.append(''.join(random.choice(choiceStr) for x in range(random.randint(0,6))))
    return ",".join(h)


def write_syn_dataset(csvPathname, rowCount, colCount, headerData=None):
    dsf = open(csvPathname, "w+")
    if headerData is not None:
        dsf.write(headerData + "\n")

    # re-randomize every row
    for i in range(rowCount):
        rowData = rand_rowData(colCount)
        dsf.write(rowData + "\n")
    dsf.close()

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
            pass
        h2o.init(1,java_heap_GB=2,use_flatfile=True)
        #h2b.browseTheCloud()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud(h2o.nodes)
    
    def test_parse_time_rand_fvec_NOPASS(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        csvFilename = "syn_time.csv"
        csvPathname = SYNDATASETS_DIR + '/' + csvFilename

        colCount = 6
        rowCount = 10
        headerData = rand_header(colCount)
        write_syn_dataset(csvPathname, rowCount, colCount, headerData)

        for trial in range (1):
            rowData = rand_rowData()
            # make sure all key names are unique, when we re-put and re-parse (h2o caching issues)
            src_key = csvFilename + "_" + str(trial)
            hex_key = csvFilename + "_" + str(trial) + ".hex"

            start = time.time()
            parseResultA = h2i.import_parse(path=csvPathname, schema='put', src_key=src_key, hex_key=hex_key)
            print "\nA trial #", trial, "parse end on ", csvFilename, 'took', time.time() - start, 'seconds'
            inspect = h2o_cmd.runInspect(key=hex_key)
            numRowsA = inspect['numRows']
            numColsA = inspect['numCols']

            summaryResult = h2o_cmd.runSummary(key=hex_key, timeoutSecs=100,
                numCols=numColsA, numRows=numRowsA, noPrint=True)

            print summaryResult
            h2o_cmd.infoFromSummary(summaryResult)
            (missingValuesDictA, constantValuesDictA, enumSizeDictA, colTypeDictA, colNameDictA) = \
                h2o_cmd.columnInfoFromInspect(hex_key, exceptionOnMissingValues=False)


            if constantValuesDictA or enumSizeDictA:
                raise Exception("Should be empty?  constantValuesDictA %s enumSizeDictA %s" % (constantValuesDictA, enumSizeDictA))

            print "missingValuesListA", missingValuesListA

            # self.assertEqual(missingValuesListA, [], "missingValuesList should be empty")
            self.assertEqual(numColsA, colCount)
            self.assertEqual(numRowsA, rowCount)

            # do a little testing of saving the key as a csv
            csvDownloadPathname = SYNDATASETS_DIR + "/csvDownload.csv"
            h2o.nodes[0].csv_download(src_key=hex_key, csvPathname=csvDownloadPathname)

            # remove the original parsed key. source was already removed by h2o
            h2o.nodes[0].remove_key(hex_key)
            # interesting. what happens when we do csv download with time data?
            start = time.time()
            parseResultB = h2i.import_parse(path=csvDownloadPathname, schema='put', src_key=src_key, hex_key=hex_key)
            print "B trial #", trial, "parse end on ", csvFilename, 'took', time.time() - start, 'seconds'
            inspect = h2o_cmd.runInspect(key=hex_key)
            numRowsB = inspect['numRows']
            numColsB = inspect['numCols']
            print "missingValuesListB", missingValuesListB
            summaryResult = h2o_cmd.runSummary(key=hex_key, timeoutSecs=100,
                numCols=numColsB, numRows=numRowsB, noPrint=True)
            (missingValuesDictB, constantValuesDictB, enumSizeDictB, colTypeDictB, colNameDictB) = \
                h2o_cmd.columnInfoFromInspect(hex_key, exceptionOnMissingValues=False)
            if constantValuesDictB or enumSizeDictB:
                raise Exception("Should be empty?  constantValuesDictB %s enumSizeDictB %s" % (constantValuesDictB, enumSizeDictB))

            self.assertEqual(missingValuesListA, missingValuesListB,
                "missingValuesList mismatches after re-parse of downloadCsv result")
            self.assertEqual(numColsA, numColsB,
                "numCols mismatches after re-parse of downloadCsv result")
            # H2O adds a header to the csv created. It puts quotes around the col numbers if no header
            # but in this dataset we have a header too, so the row counts should be equal
            # if not, maybe the parse of our dataset didn't detect a row
            self.assertEqual(numRowsA, numRowsB,
                "numRowsA: %s numRowsB: %s mismatch after re-parse of downloadCsv result" % (numRowsA, numRowsB) )

            # FIX! should do some comparison of values? 
            # maybe can use exec to checksum the columns and compare column list.
            # or compare to expected values? (what are the expected values for the number for time inside h2o?)

            # FIX! should compare the results of the two parses. The infoFromInspect result?
            ### h2b.browseJsonHistoryAsUrlLastMatch("Inspect")
            h2o.check_sandbox_for_errors()

if __name__ == '__main__':
    h2o.unit_main()

    


