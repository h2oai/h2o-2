import unittest, time, sys, random
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i

def write_syn_dataset(csvPathname, rowCount, colCount):
    dsf = open(csvPathname, "w+")
    # add the all 0's and all 1's case, so no cols get dropped due to constant 0 or 1 !
    # 0's plus right output
    rowData = [0 for i in range(colCount)]
    rowData.append(0)
    rowData = ','.join(map(str, rowData)) + "\n"
    dsf.write(rowData)

    # 1's plus right output
    rowData = [1 for i in range(colCount)]
    rowData.append(colCount % 2) # it's an inclusive xor of 1, length colCount!
    rowData = ','.join(map(str, rowData)) + "\n"
    dsf.write(rowData)

    # subtract 2 from the rows we want
    for j in range(rowCount-2):
        # just random 0 and 1
        rowData = [random.randint(0,1023) for i in range(colCount)]
        rowSum = sum(rowData)
        # output depends on all the inputs (xor)
        # rowData.append(rowSum % 2) 
        rowData.append(random.randint(0,253))
        # 32 output classes
        # rowData.append(rowSum % 32) 
        rowData = ','.join(map(str, rowData)) + "\n"
        dsf.write(rowData)

    dsf.close()

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init(2, java_heap_GB=7)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()
    
    def test_rf_big_rand_tree_fvec(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        csvFilename = "syn.csv"
        csvPathname = SYNDATASETS_DIR + '/' + csvFilename

        rowCount = 5000
        colCount = 1000
        write_syn_dataset(csvPathname, rowCount, colCount)

        for trial in range (1):
            # make sure all key names are unique, when we re-put and re-parse (h2o caching issues)
            src_key = csvFilename + "_" + str(trial)
            hex_key = csvFilename + "_" + str(trial) + ".hex"
            seed = random.randint(0,sys.maxint)
            # some cols can be dropped due to constant 0 or 1. make sure data set has all 0's and all 1's above
            # to guarantee no dropped cols!
            # kwargs = {'ntree': 3, 'depth': 50, 'seed': seed}
            # out of memory/GC errors with the above. reduce depth
            kwargs = {'ntrees': 3, 'max_depth': 20, 'seed': seed}
            start = time.time()
            parseResult = h2i.import_parse(path=csvPathname, schema='put', hex_key=hex_key, timeoutSecs=90)
            h2o_cmd.runRF(parseResult=parseResult, timeoutSecs=600, pollTimeoutSecs=180, **kwargs)
            print "trial #", trial, "rowCount:", rowCount, "colCount:", colCount, "RF end on ", csvFilename, \
                'took', time.time() - start, 'seconds'

            inspect = h2o_cmd.runInspect(key=hex_key)
            h2o_cmd.infoFromInspect(inspect, csvPathname)

            cols = inspect['cols']
            numCols = inspect['numCols']
            for i,c in enumerate(cols):
                colType = c['type']
                self.assertEqual(colType, 'Int', msg="col %d should be type in: %s" % (i, colType))

            h2o.check_sandbox_for_errors()

if __name__ == '__main__':
    h2o.unit_main()
