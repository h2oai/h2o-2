import unittest, time, sys, random
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd
import h2o_browse as h2b

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
        rowData = ','.join(map(str, rowData)) + "\n"
        dsf.write(rowData)

    dsf.close()

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED, localhost
        SEED = h2o.setup_random_seed()
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(1, java_heap_GB=14)
        else:
            import h2o_hosts
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()
    
    def test_rf_big_rand_trees(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        csvFilename = "syn.csv"
        csvPathname = SYNDATASETS_DIR + '/' + csvFilename

        rowCount = 1000
        colCount = 1000
        write_syn_dataset(csvPathname, rowCount, colCount)

        for trial in range (1):
            # make sure all key names are unique, when we re-put and re-parse (h2o caching issues)
            key = csvFilename + "_" + str(trial)
            key2 = csvFilename + "_" + str(trial) + ".hex"
            # no restriction on depth
            seed = random.randint(0,sys.maxint)
            # some cols can be dropped due to constant 0 or 1. make sure data set has all 0's and all 1's above
            # to guarantee no dropped cols!
            kwargs = {'ntree': 3, 'depth': None, 'seed': seed, 'features': colCount}
            start = time.time()
            key = h2o_cmd.runRF(csvPathname=csvPathname, key=key, key2=key2, 
                timeoutSecs=60, pollTimeoutSecs=5, **kwargs)
            print "trial #", trial, "rowCount:", rowCount, "colCount:", colCount, "RF end on ", csvFilename, \
                'took', time.time() - start, 'seconds'

            inspect = h2o_cmd.runInspect(key=key2)
            cols = inspect['cols']
            num_cols = inspect['num_cols']
            for i,c in enumerate(cols):
                colType = c['type']
                colSize = c['size']
                self.assertEqual(colType, 'int', msg="col %d should be type in: %s" % (i, colType))
                self.assertEqual(colSize, 1, msg="col %d should be size 1: %d" % (i, colSize))

            h2o.check_sandbox_for_errors()

if __name__ == '__main__':
    h2o.unit_main()
