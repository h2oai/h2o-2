import unittest, time, sys, random
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i, h2o_rf

# fyi max FP single precision (hex rep. 7f7f ffff) approx. 3.4028234 * 10**38"
print "https://0xdata.atlassian.net/browse/PUB-235"
print "datasets with reals outside the range of single precision FP exponents cause DRF1 to ignore cols."
print "No warning in browser or json about the ignored cols (h2o stdout log WARN only)"

def write_syn_dataset(csvPathname, rowCount, colCount, headerData):
    dsf = open(csvPathname, "w+")
    
    # output is just 0 or 1 randomly
    dsf.write(headerData + "\n")
    # add random output. just 0 or 1
    for i in range(rowCount):
        rowData = rand_rowData(colCount)
        dsf.write(rowData + "," + str(random.randint(0,1)) + "\n")
    dsf.close()

# append!
def append_syn_dataset(csvPathname, colCount, num):
    with open(csvPathname, "a") as dsf:
        for i in range(num):
            rowData = rand_rowData(colCount)
            dsf.write(rowData + "\n")

def rand_rowData(colCount):
    # UPDATE: maybe because of byte buffer boundary issues, single byte
    # data is best? if we put all 0s or 1, then I guess it will be bits?

    # see https://0xdata.atlassian.net/browse/HEX-1315 for problem if -1e59,1e59 range is used
    # keep values in range +/- ~10e-44.85 to ~10e38.53 to fit in SP exponent range

    rowData = str(random.uniform(0,colCount))
    for i in range(colCount):
        # h2o used to only support single (HEX-638)
        if i == 1:
            rowData = rowData + "," + str(random.randint(0,1))
            continue
        rowData = rowData + "," + str(random.uniform(-1e59,1e59))

        # rowData = rowData + "," + str(random.uniform(-1e37,1e37))
    return rowData

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init(1,java_heap_MB=1300,use_flatfile=True)

    @classmethod
    def tearDownClass(cls):
        if not h2o.browse_disable:
            # time.sleep(500000)
            pass

        h2o.tear_down_cloud()
    
    def test_rf_float_bigexp_fvec(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        csvFilename = "syn_prostate.csv"
        csvPathname = SYNDATASETS_DIR + '/' + csvFilename

        headerData = "ID,CAPSULE,AGE,RACE,DPROS,DCAPS,PSA,VOL,GLEASON"
        totalRows = 1000
        colCount = 7
        write_syn_dataset(csvPathname, totalRows, colCount, headerData)

        for trial in range (5):
            # grow the data set
            rowData = rand_rowData(colCount)
            num = random.randint(4096, 10096)
            append_syn_dataset(csvPathname, colCount, num)
            totalRows += num

            # make sure all key names are unique, when we re-put and re-parse (h2o caching issues)
            hex_key = csvFilename + "_" + str(trial) + ".hex"
            ntree = 2
            kwargs = {
                'response': 'AGE',
                'ntrees': ntree,
                'mtries': None,
                'max_depth': 20,
                'sample_rate': 0.67,
                'destination_key': None,
                'nbins': 1024,
                'seed': 784834182943470027,
            }
            parseResult = h2i.import_parse(path=csvPathname, schema='put', hex_key=hex_key, doSummary=True)

            start = time.time()
            rfView = h2o_cmd.runSpeeDRF(parseResult=parseResult, timeoutSecs=15, pollTimeoutSecs=15, **kwargs)
            print "trial #", trial, "totalRows:", totalRows, "num:", num, "RF end on ", csvFilename, \
                'took', time.time() - start, 'seconds'
            rfView["drf_model"] = rfView.pop("speedrf_model")
            (classification_error, classErrorPctList, totalScores) = h2o_rf.simpleCheckRFView(None, rfView, ntree=ntree)

            inspect = h2o_cmd.runInspect(key=hex_key)
            cols = inspect['cols']
            #num_cols = inspect['num_cols']
            #for i,c in enumerate(cols):
            #    if i < (num_cols-1): # everything except the last col (output) should be 8 byte float
            #        colType = c['type']
            #        self.assertEqual(colType, 'float', msg="col %d should be type Real: %s" % (i, colType))
        
            h2o.check_sandbox_for_errors()


if __name__ == '__main__':
    h2o.unit_main()
