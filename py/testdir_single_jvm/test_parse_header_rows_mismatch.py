import unittest, time, sys, random
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_import as h2i
import h2o_browse as h2b

def write_syn_dataset(csvPathname, rowCount, headerData, rList):
    dsf = open(csvPathname, "w+")
    
    dsf.write(headerData + "\n")
    for i in range(rowCount):
        # two choices on the input. Make output choices random
        r = rList[random.randint(0,1)] + "," + str(random.randint(0,7))
        dsf.write(r + "\n")
    dsf.close()

def rand_rowData(totalCols):
    rowData1 = str(random.randint(0,7))
    for i in range(totalCols):
        rowData1 = rowData1 + "," + str(random.randint(0,7))

    rowData2 = str(random.randint(0,7))
    for i in range(totalCols):
        rowData2 = rowData2 + "," + str(random.randint(0,7))
    # RF will complain if all inputs are the same
    r = [rowData1, rowData2]
    return r

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED, localhost
        SEED = h2o.setup_random_seed()
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(2,java_heap_MB=1300,use_flatfile=True)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud(h2o.nodes)
    
    def test_parse_header_rows_mismatch(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        csvFilename = "syn_ints.csv"
        csvPathname = SYNDATASETS_DIR + '/' + csvFilename

        # headerData = "ID,CAPSULE,AGE,RACE,DPROS,DCAPS,PSA,VOL,GLEASON"
        headerData = "ID,CAPSULE,AGE,RACE,DPROS,DCAPS,PSA,VOL"

        totalCols = 8
        totalRows = 10000
        rList = rand_rowData(totalCols)
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
            print 'parse time:', parseResult['response']['time']

            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
            h2o_cmd.infoFromInspect(inspect, csvPathname)
            print "\n" + csvPathname, \
                "    num_rows:", "{:,}".format(inspect['num_rows']), \
                "    num_cols:", "{:,}".format(inspect['num_cols'])
            # should match # of cols in header or ??
            self.assertEqual(inspect['num_cols'], totalCols, 
                "parse created result with the wrong number of cols %s %s" % (inspect['num_cols'], totalCols))
            self.assertEqual(inspect['num_rows'], totalRows,
                "parse created result with the wrong number of rows (header shouldn't count) %s %s" % (inspect['num_rows'], totalRows))


            kwargs = {'sample': 75, 'depth': 25, 'ntree': 1}
            start = time.time()
            rfv = h2o_cmd.runRF(parseResult=parseResult, timeoutSecs=30, **kwargs)
            elapsed = time.time() - start
            print "%d pct. of timeout" % ((elapsed/timeoutSecs) * 100)
            print "trial #", trial, "totalRows:", totalRows, "parse end on ", csvFilename, \
                'took', time.time() - start, 'seconds'

            h2o.check_sandbox_for_errors()

if __name__ == '__main__':
    h2o.unit_main()
