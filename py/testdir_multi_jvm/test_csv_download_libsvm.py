import unittest, random, sys, time
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import as h2i, h2o_exec as h2e
import h2o_util

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
        global SEED, localhost
        SEED = h2o.setup_random_seed()
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(2,java_heap_GB=7)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_csv_download_libsvm(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        tryList = [
            (5000, 10000, 'cK', 60),
            (10000, 10000, 'cL', 60),
            (50000, 10000, 'cM', 60),
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
            parseResultA = h2i.import_parse(path=csvPathname, schema='put', hex_key=hex_key, timeoutSecs=timeoutSecs)
            print "\nA Trial #", trial, "rowCount:", rowCount, "colCount:", colCount, "parse end on ", \
                csvFilename, 'took', time.time() - start, 'seconds'

            inspect = h2o_cmd.runInspect(key=hex_key, timeoutSecs=timeoutSecs)
            missingValuesListA = h2o_cmd.infoFromInspect(inspect, csvPathname)
            num_colsA = inspect['num_cols']
            num_rowsA = inspect['num_rows']
            row_sizeA = inspect['row_size']
            value_size_bytesA = inspect['value_size_bytes']

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
            num_colsB = inspect['num_cols']
            num_rowsB = inspect['num_rows']
            row_sizeB = inspect['row_size']
            value_size_bytesB = inspect['value_size_bytes']

            self.assertEqual(missingValuesListA, missingValuesListB,
                "missingValuesList mismatches after re-parse of downloadCsv result")
            self.assertEqual(num_colsA, num_colsB,
                "num_cols mismatches after re-parse of downloadCsv result %d %d" % (num_colsA, num_colsB))
            self.assertEqual(num_rowsA, num_rowsB,
                "num_rows mismatches after re-parse of downloadCsv result %d %d" % (num_rowsA, num_rowsB))
            self.assertEqual(row_sizeA, row_sizeB,
                "row_size mismatches after re-parse of downloadCsv result %d %d" % (row_sizeA, row_sizeB))
            self.assertEqual(value_size_bytesA, value_size_bytesB,
                "value_size_bytes mismatches after re-parse of downloadCsv result %d %d" % (value_size_bytesA, value_size_bytesB))

            h2o.check_sandbox_for_errors()


if __name__ == '__main__':
    h2o.unit_main()
