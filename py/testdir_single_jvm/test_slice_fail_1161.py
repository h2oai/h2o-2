import unittest, random, sys, time
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_rf as h2o_rf, h2o_hosts, h2o_import as h2i, h2o_exec
import h2o_browse as h2b

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(node_count=1, java_heap_GB=10)
        else:
            h2o_hosts.build_cloud_with_hosts(node_count=1, java_heap_GB=10)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_slice_fail_1161(self):
        print "\nUse randomBitVector and filter to separate the dataset randomly"
        importFolderPath = "standard"
        csvFilename = 'covtype.data'
        csvPathname = importFolderPath + "/" + csvFilename
        hex_key = csvFilename + ".hex"

        print "\nUsing header=0 on the normal covtype.data"
        parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, hex_key=hex_key,
            header=0, timeoutSecs=100)

        inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
        print "\n" + csvPathname, \
            "    num_rows:", "{:,}".format(inspect['num_rows']), \
            "    num_cols:", "{:,}".format(inspect['num_cols'])

        # how many rows for each pct?
        num_rows = inspect['num_rows']
        pct10 = int(num_rows * .1)
        rowsForPct = [i * pct10 for i in range(0,11)]
        # this can be slightly less than 10%
        last10 = num_rows - rowsForPct[9]
        rowsForPct[10] = last10
        # use mod below for picking "rows-to-do" in case we do more than 9 trials
        # use 10 if 0 just to see (we copied 10 to 0 above)
        rowsForPct[0] = rowsForPct[10]

        print "Creating the key of the last 10% data, for scoring"
        dataKeyTest = "rTest"
        dataKeyTrain = "rTrain"
        # start at 90% rows + 1

        for trial in range(1,10):
            # odd. output is byte, all other exec outputs are 8 byte? (at least the ones below?)
            execExpr = "rbv=randomBitVector(" + str(num_rows) + "," + str(last10) + ",12345)"
            h2o_exec.exec_expr(None, execExpr, resultKey="rbv", timeoutSecs=10)

            # complement the bit vector
            execExpr = "not_rbv=colSwap(rbv,0,rbv[0]==0?1:0)"
            h2o_exec.exec_expr(None, execExpr, resultKey="not_rbv", timeoutSecs=10)

            execExpr = dataKeyTest + "=filter(" + hex_key + ",rbv)"
            h2o_exec.exec_expr(None, execExpr, resultKey=dataKeyTest, timeoutSecs=10)

            execExpr = dataKeyTrain + "=filter(" + hex_key + ",not_rbv)"
            h2o_exec.exec_expr(None, execExpr, resultKey=dataKeyTrain, timeoutSecs=10)
            
            rowsToUse = rowsForPct[trial%10] 
            resultKey = "r" + str(trial)
            # different fails for specifying row count or not
            execExpr = resultKey + "=slice(" + dataKeyTrain + ",0," + str(rowsToUse) + ")"
            h2o_exec.exec_expr(None, execExpr, resultKey=resultKey, timeoutSecs=10)


if __name__ == '__main__':
    h2o.unit_main()
