import unittest, time, sys, random
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i

def write_syn_dataset(csvPathname, rowCount, headerData, rowData):
    dsf = open(csvPathname, "w+")
    
    dsf.write(headerData + "\n")
    for i in range(rowCount):
        dsf.write(rowData + "\n")
    dsf.close()

# append!
def append_syn_dataset(csvPathname, rowData, num):
    with open(csvPathname, "a") as dsf:
        for i in range(num):
            dsf.write(rowData + "\n")

def rand_rowData():
    # UPDATE: maybe because of byte buffer boundary issues, single byte
    # data is best? if we put all 0s or 1, then I guess it will be bits?
    rowData = str(random.uniform(0,7))
    for i in range(8):
        rowData = rowData + "," + str(random.uniform(-1e59,1e59))
    return rowData

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED, localhost
        SEED = h2o.setup_random_seed()
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(2,java_heap_GB=7,use_flatfile=True)
        else:
            import h2o_hosts
            h2o_hosts.build_cloud_with_hosts()
        h2b.browseTheCloud()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud(h2o.nodes)
    
    def test_parse_csv_download(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        csvFilename = "syn_prostate.csv"
        csvPathname = SYNDATASETS_DIR + '/' + csvFilename

        headerData = "ID,CAPSULE,AGE,RACE,DPROS,DCAPS,PSA,VOL,GLEASON"

        rowData = rand_rowData()
        totalRows = 1000000
        write_syn_dataset(csvPathname, totalRows, headerData, rowData)

        print "This is the same format/data file used by test_same_parse, but the non-gzed version"
        print "\nSchmoo the # of rows"
        # failed around 50 trials..python memory problem
        for trial in range (20):
            rowData = rand_rowData()
            num = random.randint(4096, 10096)
            append_syn_dataset(csvPathname, rowData, num)
            totalRows += num

            # make sure all key names are unique, when we re-put and re-parse (h2o caching issues)
            src_key = csvFilename + "_" + str(trial)
            hex_key = csvFilename + "_" + str(trial) + ".hex"

            start = time.time()
            parseResultA = h2i.import_parse(path=csvPathname, schema='put', src_key=src_key, hex_key=hex_key)
            print "\nA trial #", trial, "totalRows:", totalRows, "parse end on ", \
                csvFilename, 'took', time.time() - start, 'seconds'

            inspect = h2o_cmd.runInspect(key=hex_key)
            missingValuesListA = h2o_cmd.infoFromInspect(inspect, csvPathname)
            num_colsA = inspect['num_cols']
            num_rowsA = inspect['num_rows']
            row_sizeA = inspect['row_size']
            value_size_bytesA = inspect['value_size_bytes']

            # do a little testing of saving the key as a csv
            csvDownloadPathname = SYNDATASETS_DIR + "/csvDownload.csv"
            h2o.nodes[0].csv_download(src_key=hex_key, csvPathname=csvDownloadPathname)

            # remove the original parsed key. source was already removed by h2o
            h2o.nodes[0].remove_key(hex_key)
            start = time.time()
            parseResultB = h2i.import_parse(path=csvDownloadPathname, schema='put', src_key=src_key, hex_key=hex_key)
            print "B trial #", trial, "totalRows:", totalRows, "parse end on ", \
                csvFilename, 'took', time.time() - start, 'seconds'
            inspect = h2o_cmd.runInspect(key=hex_key)
            missingValuesListB = h2o_cmd.infoFromInspect(inspect, csvPathname)
            num_colsB = inspect['num_cols']
            num_rowsB = inspect['num_rows']
            row_sizeB = inspect['row_size']
            value_size_bytesB = inspect['value_size_bytes']

            self.assertEqual(missingValuesListA, missingValuesListB, 
                "missingValuesList mismatches after re-parse of downloadCsv result")
            self.assertEqual(num_colsA, num_colsB, 
                "num_cols mismatches after re-parse of downloadCsv result")
            self.assertEqual(num_rowsA, num_rowsB, 
                "num_rows mismatches after re-parse of downloadCsv result")
            self.assertEqual(row_sizeA, row_sizeB, 
                "row_size mismatches after re-parse of downloadCsv result")
            self.assertEqual(value_size_bytesA, value_size_bytesB, 
                "value_size_bytes mismatches after re-parse of downloadCsv result")

            h2o.check_sandbox_for_errors()

if __name__ == '__main__':
    h2o.unit_main()
