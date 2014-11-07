import unittest, random, sys, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i, h2o_exec as h2e
import h2o_util

DO_BYTESIZE_COMPARE = False

def write_syn_dataset(csvPathname, rowCount, colCount, SEEDPERFILE):
    start = time.time()
    r = random.Random(SEEDPERFILE)
    dsf = open(csvPathname, "w+")
    colNumberMax = 0
    for i in range(rowCount):
        rowData = []
        if i==(rowCount-1): # last row
            val = 1.0
            colNumber = colCount # max
        else:
            # 50%
            d = random.randint(0,1)
            if d==1:
                val = 1.0
            else:
                val = 0
            colNumber = 1 # always make it col 1

        if val!=0:
            rowData.append(str(colNumber) + ":" + str(val))
            colNumberMax = max(colNumber, colNumberMax)

        # add output
        val = 0
        rowData.insert(0, val)
        rowDataCsv = " ".join(map(str,rowData))
        dsf.write(rowDataCsv + "\n")

    dsf.close()
    print '\n', csvPathname, 'creation took', time.time() - start, 'seconds'
    return colNumberMax

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init(2,java_heap_GB=7)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_csv_download_libsvm(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        tryList = [
            (5000, 10000, 'cK', 120),
            (10000, 10000, 'cL', 120),
            (50000, 10000, 'cM', 300),
            ]

        ### h2b.browseTheCloud()
        lenNodes = len(h2o.nodes)

        trial = 0
        for (rowCount, colCount, hex_key, timeoutSecs) in tryList:
            trial += 1
            csvFilename = 'syn_' + str(SEED) + "_" + str(rowCount) + 'x' + str(colCount) + '.csv'
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename

            print "Creating random", csvPathname
            write_syn_dataset(csvPathname, rowCount, colCount, SEED)

            start = time.time()
            # Summary is kind of slow. should I do it separately
            parseResultA = h2i.import_parse(path=csvPathname, schema='put', hex_key=hex_key, timeoutSecs=timeoutSecs, doSummary=False)
            print "\nA Trial #", trial, "rowCount:", rowCount, "colCount:", colCount, "parse end on ", \
                csvFilename, 'took', time.time() - start, 'seconds'

            inspect = h2o_cmd.runInspect(key=hex_key, timeoutSecs=timeoutSecs)
            missingValuesListA = h2o_cmd.infoFromInspect(inspect, csvPathname)
            numColsA = inspect['numCols']
            numRowsA = inspect['numRows']
            byteSizeA = inspect['byteSize']

            # do a little testing of saving the key as a csv
            csvDownloadPathname = SYNDATASETS_DIR + "/csvDownload.csv"
            print "\nStarting csv download to",  csvDownloadPathname, "rowCount:", rowCount, "colCount:", colCount
            start = time.time()
            h2o.nodes[0].csv_download(src_key=hex_key, csvPathname=csvDownloadPathname)
            print "csv_download end.", 'took', time.time() - start, 'seconds. Originally from:', csvFilename

            # remove the original parsed key. source was already removed by h2o
            h2o.nodes[0].remove_key(hex_key)
            start = time.time()
            parseResultB = h2i.import_parse(path=csvDownloadPathname, schema='put', hex_key=hex_key, timeoutSecs=timeoutSecs)
            print "\nB Trial #", trial, "rowCount:", rowCount, "colCount:", colCount, "parse end on ", \
                csvFilename, 'took', time.time() - start, 'seconds'
            inspect = h2o_cmd.runInspect(key=hex_key, timeoutSecs=timeoutSecs)
            missingValuesListB = h2o_cmd.infoFromInspect(inspect, csvPathname)
            numColsB = inspect['numCols']
            numRowsB = inspect['numRows']
            byteSizeB = inspect['byteSize']

            self.assertEqual(missingValuesListA, missingValuesListB,
                "missingValuesList mismatches after re-parse of downloadCsv result")
            self.assertEqual(numColsA, numColsB,
                "numCols mismatches after re-parse of downloadCsv result %d %d" % (numColsA, numColsB))
            self.assertEqual(numRowsA, numRowsB,
                "numRows mismatches after re-parse of downloadCsv result %d %d" % (numRowsA, numRowsB))

            if DO_BYTESIZE_COMPARE:
                self.assertEqual(byteSizeA, byteSizeB,
                    "byteSize mismatches after re-parse of downloadCsv result %d %d" % (byteSizeA, byteSizeB))

            h2o.check_sandbox_for_errors()


if __name__ == '__main__':
    h2o.unit_main()
