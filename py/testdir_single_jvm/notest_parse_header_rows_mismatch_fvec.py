import unittest, time, sys, random
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_import as h2i
import h2o_browse as h2b

def write_syn_dataset(csvPathname, rowCount, headerData, rList):
    dsf = open(csvPathname, "w+")
    
    dsf.write(headerData + "\n")
    for i in range(rowCount):
        # two choices on the input. Make output choices random
        r = rList[random.randint(0,1)] + "," + str(random.randint(0,7))
        dsf.write(r + "\n")
    dsf.close()

def rand_rowData(inputCols):
    rowData1 = [str(random.randint(0,7)) for i in range(inputCols)]
    rowData2 = [str(random.randint(0,7)) for i in range(inputCols)]

    # make them comma separated strings now, and return the pair
    r = [",".join(rowData1), ",".join(rowData2)]
    print "\nrandom row 0:", r[0]
    print "random row 1:", r[1]
    return r

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init(2,java_heap_MB=1300,use_flatfile=True)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud(h2o.nodes)
    
    def test_NOPASS_parse_header_rows_mismatch_fvec(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        csvFilename = "syn_ints.csv"
        csvPathname = SYNDATASETS_DIR + '/' + csvFilename

        # headerData = "ID,CAPSULE,AGE,RACE,DPROS,DCAPS,PSA,VOL,GLEASON"
        # note the output response col doesn't have a header..h2o currently drops the col if so
        headerData = "ID,CAPSULE,AGE,RACE,DPROS,DCAPS,PSA,VOL"

        inputCols = 8
        totalRows = 10000
        rList = rand_rowData(inputCols)
        write_syn_dataset(csvPathname, totalRows, headerData, rList)

        for trial in range (2):
            # make sure all key names are unique, when we re-put and re-parse (h2o caching issues)
            src_key = csvFilename + "_" + str(trial)
            hex_key = csvFilename + "_" + str(trial) + ".hex"

            start = time.time()
            timeoutSecs = 30
            print "Force it to think there's a header. using comma forced as separator"
            parseResult = h2i.import_parse(path=csvPathname, src_key=src_key, schema='put', hex_key=hex_key,
                timeoutSecs=timeoutSecs, pollTimeoutSecs=30, header=1, separator=44)
            print "parseResult['destination_key']: " + parseResult['destination_key']

            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
            h2o_cmd.infoFromInspect(inspect, csvPathname)
            print "\n" + csvPathname, \
                "    numRows:", "{:,}".format(inspect['numRows']), \
                "    numCols:", "{:,}".format(inspect['numCols'])
            # the header matches niputCols. Inspect should return inputCols+1 for the output 
            # the output is appended in write_syn_dataset
        
            expectedCols = inputCols + 1
            self.assertEqual(inspect['numCols'], expectedCols,
                "parse created result with the wrong number of cols %s %s" % (inspect['numCols'], expectedCols))
            self.assertEqual(inspect['numRows'], totalRows,
                "parse created result with the wrong number of rows (header shouldn't count) %s %s" % (inspect['numRows'], totalRows))


            kwargs = {'sample_rate': 0.75, 'max_depth': 25, 'ntrees': 1}
            start = time.time()
            rfv = h2o_cmd.runRF(parseResult=parseResult, timeoutSecs=30, **kwargs)
            elapsed = time.time() - start
            print "%d pct. of timeout" % ((elapsed/timeoutSecs) * 100)
            print "trial #", trial, "totalRows:", totalRows, "parse end on ", csvFilename, \
                'took', time.time() - start, 'seconds'

            h2o.check_sandbox_for_errors()

if __name__ == '__main__':
    h2o.unit_main()
