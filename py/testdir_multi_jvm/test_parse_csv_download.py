import os, json, unittest, time, shutil, sys
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd
import h2o_browse as h2b
import random


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

class parse_rand_schmoo(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(2,java_heap_GB=10,use_flatfile=True)
        else:
            import h2o_hosts
            h2o_hosts.build_cloud_with_hosts()
        h2b.browseTheCloud()

    @classmethod
    def tearDownClass(cls):
        if not h2o.browse_disable:
            # time.sleep(500000)
            pass

        h2o.tear_down_cloud(h2o.nodes)
    
    def test_sort_of_prostate_with_row_schmoo(self):
        SEED = random.randint(0, sys.maxint)
        # if you have to force to redo a test
        # SEED = 
        random.seed(SEED)
        print "\nUsing random seed:", SEED

        SYNDATASETS_DIR = h2o.make_syn_dir()
        csvFilename = "syn_prostate.csv"
        csvPathname = SYNDATASETS_DIR + '/' + csvFilename

        headerData = "ID,CAPSULE,AGE,RACE,DPROS,DCAPS,PSA,VOL,GLEASON"

        rowData = rand_rowData()
        totalRows = 1000000
        write_syn_dataset(csvPathname, totalRows, headerData, rowData)

        print "This is the same format/data file used by test_same_parse, but the non-gzed version"
        print "\nSchmoo the # of rows"
        for trial in range (100):
            rowData = rand_rowData()
            num = random.randint(4096, 10096)
            append_syn_dataset(csvPathname, rowData, num)
            totalRows += num

            # make sure all key names are unique, when we re-put and re-parse (h2o caching issues)
            key = csvFilename + "_" + str(trial)
            key2 = csvFilename + "_" + str(trial) + ".hex"

            start = time.time()
            parseKeyA = h2o_cmd.parseFile(csvPathname=csvPathname, key=key, key2=key2)
            print "\nA trial #", trial, "totalRows:", totalRows, "parse end on ", \
                csvFilename, 'took', time.time() - start, 'seconds'

            inspect = h2o_cmd.runInspect(key=key2)
            missingValuesListA = h2o_cmd.infoFromInspect(inspect, csvPathname)
            num_colsA = inspect['num_cols']
            num_rowsA = inspect['num_rows']
            row_sizeA = inspect['row_size']
            value_size_bytesA = inspect['value_size_bytes']

            # do a little testing of saving the key as a csv
            csvDownloadPathname = SYNDATASETS_DIR + "/csvDownload.csv"
            h2o.nodes[0].csv_download(key=key2, csvPathname=csvDownloadPathname)

            # remove the original parsed key. source was already removed by h2o
            h2o.nodes[0].remove_key(key2)
            start = time.time()
            parseKeyB = h2o_cmd.parseFile(csvPathname=csvDownloadPathname, key=key, key2=key2)
            print "B trial #", trial, "totalRows:", totalRows, "parse end on ", \
                csvFilename, 'took', time.time() - start, 'seconds'
            inspect = h2o_cmd.runInspect(key=key2)
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

            # FIX! should compare the results of the two parses. The infoFromInspect result?

            ### h2b.browseJsonHistoryAsUrlLastMatch("Inspect")
            h2o.check_sandbox_for_errors()

if __name__ == '__main__':
    h2o.unit_main()
