import unittest, time, sys, random
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i, h2o_rf, h2o_browse as h2b

def write_syn_dataset(csvPathname, rowCount, colCount, headerData):
    dsf = open(csvPathname, "w+")
    
    # output is just 0 or 1 randomly
    dsf.write(headerData + "\n")
    # add random integer output equal to row number. for getting lots of output classes
    # FIX! should we add gaps? what about enums?
    # maybe use 2*i so there are gaps
    for i in range(rowCount):
        rowData = rand_rowData(colCount)
        dsf.write(rowData + "," + str(2*i) + "\n")
    dsf.close()

def rand_rowData(colCount):
    # UPDATE: maybe because of byte buffer boundary issues, single byte
    # data is best? if we put all 0s or 1, then I guess it will be bits?

    # see https://0xdata.atlassian.net/browse/HEX-1315 for problem if -1e59,1e59 range is used
    # keep values in range +/- ~10e-44.85 to ~10e38.53 to fit in SP exponent range

    rowData = str(random.uniform(0,colCount))
    for i in range(colCount):
        rowData = rowData + "," + str(random.uniform(-1e5,1e5))
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
        h2o.tear_down_cloud()
    
    def test_rf_multinomial_fvec(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        csvFilename = "syn_multinomial.csv"
        csvPathname = SYNDATASETS_DIR + '/' + csvFilename

        headerData = "ID,CAPSULE,AGE,RACE,DPROS,DCAPS,PSA,VOL,GLEASON"
        totalRows = 400
        colCount = 7

        for trial in range (5):
            write_syn_dataset(csvPathname, totalRows, colCount, headerData)
            # make sure all key names are unique, when we re-put and re-parse (h2o caching issues)
            hexKey = csvFilename + "_" + str(trial) + ".hex"
            ntree = 2
            kwargs = {
                'ntrees': ntree,
                'mtries': None,
                'max_depth': 20,
                'sample_rate': 0.67,
                'destination_key': None,
                'nbins': 1024,
                'seed': 784834182943470027,
            }
            parseResult = h2i.import_parse(path=csvPathname, schema='put', hex_key=hexKey, doSummary=True)

            start = time.time()
            rfView = h2o_cmd.runRF(parseResult=parseResult, timeoutSecs=15, pollTimeoutSecs=5, **kwargs)
            print "trial #", trial, 'took', time.time() - start, 'seconds'
            (classification_error, classErrorPctList, totalScores) = h2o_rf.simpleCheckRFView(rfv=rfView, ntree=ntree)

            modelKey = rfView['drf_model']['_key']
            h2o_cmd.runScore(dataKey=parseResult['destination_key'], modelKey=modelKey, 
                vactual=colCount+1, vpredict=1, expectedAuc=0.5, doAUC=False)

            h2b.browseJsonHistoryAsUrlLastMatch("RF")




if __name__ == '__main__':
    h2o.unit_main()
