import unittest, time, sys, random, os
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_import as h2i
import h2o_browse as h2b

# for test debug
HEADER = True
dataRowsWithHeader = 0

# Don't write headerData if None (for non-header files)
# Don't write data if rowCount is None
def write_syn_dataset(csvPathname, rowCount, headerData, rList):
    dsf = open(csvPathname, "w+")
    
    if headerData is not None:
        dsf.write(headerData + "\n")

    if rowCount is not None:        
        for i in range(rowCount):
            # two choices on the input. Make output choices random
            r = rList[random.randint(0,1)] + "," + str(random.randint(0,7))
            dsf.write(r + "\n")
        dsf.close()
        return rowCount # rows done
    else:
        dsf.close()
        return 0 # rows done

def rand_rowData(colCount):
    rowData = [random.randint(0,7) for i in range(colCount)]
    rowData1= ",".join(map(str,rowData))
    rowData = [random.randint(0,7) for i in range(colCount)]
    rowData2= ",".join(map(str,rowData))
    # RF will complain if all inputs are the same
    r = [rowData1, rowData2]
    return r

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init(java_heap_GB=4,use_flatfile=True)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()
    
    def test_parse_multi_header_single_fvec(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        csvFilename = "syn_ints.csv"
        csvPathname = SYNDATASETS_DIR + '/' + csvFilename
        headerData = "ID,CAPSULE,AGE,RACE,DPROS,DCAPS,PSA,VOL,GLEASON,output"

        # cols must be 9 to match the header above, otherwise a different bug is hit
        # extra output is added, so it's 10 total
        tryList = [
            (57, 300, 9, 'cA', 60, 0),
            # try with 1-3 data lines in the header file too
            (57, 300, 9, 'cB', 60, 1),
            (57, 300, 9, 'cC', 60, 2),
            (57, 300, 9, 'cD', 60, 3),
            ]

        trial = 0
        for (fileNum, rowCount, colCount, hex_key, timeoutSecs, dataRowsWithHeader) in tryList:
            trial += 1
            # FIX! should we add a header to them randomly???
            print "Wait while", fileNum, "synthetic files are created in", SYNDATASETS_DIR
            rowxcol = str(rowCount) + 'x' + str(colCount)
            totalCols = colCount + 1 # 1 extra for output
            totalDataRows = 0
            for fileN in range(fileNum):
                csvFilename = 'syn_' + str(fileN) + "_" + str(SEED) + "_" + rowxcol + '.csv'
                csvPathname = SYNDATASETS_DIR + '/' + csvFilename
                rList = rand_rowData(colCount)
                dataRowsDone = write_syn_dataset(csvPathname, rowCount, headerData=None, rList=rList)
                totalDataRows += dataRowsDone

            # create the header file
            # can make it pass by not doing this
            if HEADER:
                csvFilename = 'syn_header_' + str(SEED) + "_" + rowxcol + '.csv'
                csvPathname = SYNDATASETS_DIR + '/' + csvFilename
                dataRowsDone = write_syn_dataset(csvPathname, dataRowsWithHeader, headerData, rList)
                totalDataRows += dataRowsDone

            # make sure all key names are unique, when we re-put and re-parse (h2o caching issues)
            src_key = "syn_" + str(trial)
            hex_key = "syn_" + str(trial) + ".hex"

            # DON"T get redirected to S3! (EC2 hack in config, remember!)
            # use it at the node level directly (because we gen'ed the files.
            # I suppose we could force the redirect state bits in h2o.nodes[0] to False, instead?:w
            # put them, rather than using import files, so this works if remote h2o is used
            # and python creates the files locally
            fileList = os.listdir(SYNDATASETS_DIR)
            for f in fileList:
                h2i.import_only(path=SYNDATASETS_DIR + "/" + f, schema='put', noPrint=True)
                print f

            if HEADER:
                header = h2i.find_key('syn_header')
                if not header:
                    raise Exception("Didn't find syn_header* key in the import")

            # use regex. the only files in the dir will be the ones we just created with  *fileN* match
            print "Header Key = " + header
            start = time.time()
            parseResult = h2i.parse_only(pattern='*'+rowxcol+'*',
                hex_key=hex_key, timeoutSecs=timeoutSecs, header="1", header_from_file=header)

            print "parseResult['destination_key']: " + parseResult['destination_key']

            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
            h2o_cmd.infoFromInspect(inspect, csvPathname)
            print "\n" + csvPathname, \
                "    numRows:", "{:,}".format(inspect['numRows']), \
                "    numCols:", "{:,}".format(inspect['numCols'])

            # should match # of cols in header or ??
            self.assertEqual(inspect['numCols'], totalCols, 
                "parse created result with the wrong number of cols %s %s" % (inspect['numCols'], totalCols))
            self.assertEqual(inspect['numRows'], totalDataRows,
                "parse created result with the wrong number of rows (header shouldn't count) %s %s" % \
                (inspect['numRows'], totalDataRows))

            # put in an ignore param, that will fail unless headers were parsed correctly
            if HEADER:
                kwargs = {'sample_rate': 0.75, 'max_depth': 25, 'ntrees': 1, 'ignored_cols_by_name': 'ID,CAPSULE'}
            else:
                kwargs = {'sample_rate': 0.75, 'max_depth': 25, 'ntrees': 1}

            start = time.time()
            rfv = h2o_cmd.runRF(parseResult=parseResult, timeoutSecs=timeoutSecs, **kwargs)
            elapsed = time.time() - start
            print "%d pct. of timeout" % ((elapsed/timeoutSecs) * 100)
            print "trial #", trial, "totalDataRows:", totalDataRows, "parse end on ", csvFilename, \
                'took', time.time() - start, 'seconds'

            h2o.check_sandbox_for_errors()

if __name__ == '__main__':
    h2o.unit_main()
