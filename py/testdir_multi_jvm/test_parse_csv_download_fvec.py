import unittest, time, sys, random
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i

# the downloaed csv will have fp precision issues relative to the first file
# so they won't be the same size when loaded back in?
# 
# ==> csvDownload.csv <==
# "ID","CAPSULE","AGE","RACE","DPROS","DCAPS","PSA","VOL","GLEASON"
# 2.53036595307,-6.01347554964e+58,-6.61152356007e+57,-5.79482460798e+58,-4.6994089693100004e+58,3.9920553266700003e+58,-6.980994970390001e+58,-1.0522636935000001e+58,-4.87409007483e+58
# 
# ==> syn_prostate.csv <==
# ID,CAPSULE,AGE,RACE,DPROS,DCAPS,PSA,VOL,GLEASON
# 2.53036595307,-6.01347554964e+58,-6.61152356007e+57,-5.79482460798e+58,-4.69940896931e+58,3.99205532667e+58,-6.98099497039e+58,-1.0522636935e+58,-4.87409007483e+58
# 
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
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init(2,java_heap_GB=7,use_flatfile=True)
        h2b.browseTheCloud()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud(h2o.nodes)
    
    def test_parse_csv_download_fvec(self):
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
        for trial in range (5):
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
            numColsA = inspect['numCols']
            numRowsA = inspect['numRows']
            byteSizeA = inspect['byteSize']

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
            numColsB = inspect['numCols']
            numRowsB = inspect['numRows']
            byteSizeB = inspect['byteSize']

            self.assertEqual(missingValuesListA, missingValuesListB, 
                "missingValuesList mismatches after re-parse of downloadCsv result")
            self.assertEqual(numColsA, numColsB, 
                "numCols mismatches after re-parse of downloadCsv result")
            self.assertEqual(numRowsA, numRowsB, 
                "numRows mismatches after re-parse of downloadCsv result")
            # self.assertEqual(byteSizeA, byteSizeB, 
            #    "byteSize mismatches after re-parse of downloadCsv result %s %s" % (byteSizeA, byteSizeB))

            h2o.check_sandbox_for_errors()

if __name__ == '__main__':
    h2o.unit_main()
