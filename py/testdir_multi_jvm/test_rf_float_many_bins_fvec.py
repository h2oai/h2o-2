import unittest, time, sys, random
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i

def write_syn_dataset(csvPathname, rowCount, headerData):
    dsf = open(csvPathname, "w+")
    
    # output is just 0 or 1 randomly
    dsf.write(headerData + "\n")
    # add random output. just 0 or 1
    for i in range(rowCount):
        rowData = rand_rowData()
        dsf.write(rowData + "," + str(random.randint(0,1)) + "\n")
    dsf.close()

# append!
def append_syn_dataset(csvPathname, num):
    with open(csvPathname, "a") as dsf:
        for i in range(num):
            rowData = rand_rowData()
            dsf.write(rowData + "\n")

def rand_rowData():
    # UPDATE: maybe because of byte buffer boundary issues, single byte
    # data is best? if we put all 0s or 1, then I guess it will be bits?
    rowData = str(random.uniform(0,7))
    for i in range(7):
        # randomize the sign, mantissa, exponent separately
        b = random.random()
        e = random.random() * 20
        s = random.randint(0,1)
        f = b * (10**e) * (s and -1 or 1)
        rowData = rowData + "," + str(f)
    return rowData

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init(2,java_heap_MB=1300,use_flatfile=True)
        ### h2b.browseTheCloud()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud(h2o.nodes)
    
    def test_rf_float_many_bins_fvec(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        csvFilename = "syn_prostate.csv"
        csvPathname = SYNDATASETS_DIR + '/' + csvFilename

        headerData = "ID,CAPSULE,AGE,RACE,DPROS,DCAPS,PSA,VOL,GLEASON"

        totalRows = 100000
        write_syn_dataset(csvPathname, totalRows, headerData)


        for trial in range (5):
            rowData = rand_rowData()
            num = random.randint(4096, 10096)
            append_syn_dataset(csvPathname, num)
            totalRows += num
            start = time.time()

            # make sure all key names are unique, when we re-put and re-parse (h2o caching issues)
            hex_key = csvFilename + "_" + str(trial) + ".hex"
            # On EC2 once we get to 30 trials or so, do we see polling hang? GC or spill of heap or ??
            kwargs = {'ntrees': 5, 'max_depth': 5, 'nbins': 10000}
            parseResult = h2i.import_parse(path=csvPathname, schema='put', hex_key=hex_key)
            h2o_cmd.runRF(parseResult=parseResult, timeoutSecs=10, pollTimeoutSecs=5, **kwargs)
            print "trial #", trial, "totalRows:", totalRows, "num:", num, "RF end on ", csvFilename, \
                'took', time.time() - start, 'seconds'
            ### h2o_cmd.runInspect(key=hex_key)
            ### h2b.browseJsonHistoryAsUrlLastMatch("Inspect")
            h2o.check_sandbox_for_errors()


if __name__ == '__main__':
    h2o.unit_main()
