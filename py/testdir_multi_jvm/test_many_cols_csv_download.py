import unittest, random, sys, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i, h2o_exec as h2e

def write_syn_dataset(csvPathname, rowCount, colCount, SEED):
    r1 = random.Random(SEED)
    start = time.time()
    dsf = open(csvPathname, "w+")

    # write a header to avoid or match the h2o generated header in downloadCsv
    rowData = [ "c" + str(j) for j in range(colCount)]
    rowDataCsv = ",".join(rowData)
    dsf.write(rowDataCsv + "\n")

    for i in range(rowCount):
        rowData = range(colCount)
        rowDataCsv = ",".join(map(str,rowData))
        dsf.write(rowDataCsv + "\n")

    dsf.close()
    print '\n', csvPathname, 'creation took', time.time() - start, 'seconds'


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

    def test_many_cols_csv_download(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        tryList = [
            # nrow  ncol   key   timeoutSecs
            (100,   10000, 'cC', 60),
            (1000,  100,   'cD', 60),
            (10000, 100,   'cG', 60),
            (11000, 2000,  'cI', 60),
            #
            # Note:  10000 x 10000 works, but is slow.  disable for now.
            # (10000, 10000, 'cJ', 60),
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
            parseResultA = h2i.import_parse(path=csvPathname, schema='put', hex_key=hex_key, timeoutSecs=2*timeoutSecs)
            print "\nA Trial #", trial, "rowCount:", rowCount, "colCount:", colCount, "parse end on ", \
                csvFilename, 'took', time.time() - start, 'seconds'

            inspect = h2o_cmd.runInspect(key=hex_key)
            missingValuesListA = h2o_cmd.infoFromInspect(inspect, csvPathname)
            numColsA = inspect['numCols']
            numRowsA = inspect['numRows']
            byteSizeA = inspect['byteSize']

            # do a little testing of saving the key as a csv
            csvDownloadPathname = SYNDATASETS_DIR + "/csvDownload.csv"
            print "\nStarting csv download to",  csvDownloadPathname, "rowCount:", rowCount, "colCount:", colCount
            start = time.time()
            h2o.nodes[0].csv_download(src_key=hex_key, csvPathname=csvDownloadPathname, timeoutSecs=5*timeoutSecs)
            print "csv_download end.", 'took', time.time() - start, 'seconds. Originally from:', csvFilename

            # remove the original parsed key. source was already removed by h2o
            h2o.nodes[0].remove_key(hex_key)
            start = time.time()
            parseResultB = h2i.import_parse(path=csvDownloadPathname, schema='put', hex_key=hex_key, timeoutSecs=3*timeoutSecs)
            print "\nB Trial #", trial, "rowCount:", rowCount, "colCount:", colCount, "parse end on ", \
                csvFilename, 'took', time.time() - start, 'seconds'
            inspect = h2o_cmd.runInspect(key=hex_key)
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
            # self.assertEqual(byteSizeA, byteSizeB,
            #    "byteSize mismatches after re-parse of downloadCsv result %d %d" % (byteSizeA, byteSizeB))
            h2o.check_sandbox_for_errors()

if __name__ == '__main__':
    h2o.unit_main()
