import unittest, random, sys, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_import as h2i, h2o_cmd, h2o_exec as h2e

exprList = [
    "Last.value.0 = cbind(df,indx)",
    "Last.value.1 = Last.value.0[,c(151)]",
    "Last.value.2 = Last.value.0[,151]",
    "Last.value.3 = Last.value.2 == 1",
    "Last.value.4 = Last.value.0[Last.value.3,]",
    "df.train = Last.value.4",
    "Last.value.5 = Last.value.0[,151]",
    "Last.value.6 = Last.value.5 == 0",
    "Last.value.7 = Last.value.0[Last.value.6,]",
    "df.test = Last.value.7 ",
    "Last.value.8 = indx[,c(1)]",
    "Last.value.9 = Last.value.0[,c(151)] = Last.value.8",
    #"Last.value.0[,c(151)] = Last.value.8",
    # "Last.value.0[,151] = indx[,1]",
    # "Last.value.0[,151] = Last.value.8",
]

def write_syn_dataset(csvPathname, rowCount, colCount, SEED):
    r1 = random.Random(SEED)
    dsf = open(csvPathname, "w+")

    for i in range(rowCount):
        rowData = []
        for j in range(colCount):
            ri = r1.uniform(1,129)
            rowData.append(ri)

        rowDataCsv = ",".join(map(str,rowData))
        dsf.write(rowDataCsv + "\n")

    dsf.close()


class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init(java_heap_GB=14)


    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_exec2_cbind_like_R(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()


        SEEDPERFILE = random.randint(0, sys.maxint)
        rowCount = 30000
        colCount = 150
        timeoutSecs = 60
        hex_key = "df"
        csvPathname = SYNDATASETS_DIR + "/" + "df.csv"
        write_syn_dataset(csvPathname, rowCount, colCount, SEEDPERFILE)
        parseResult = h2i.import_parse(path=csvPathname, schema='put', 
            hex_key=hex_key, timeoutSecs=3000, retryDelaySecs=2, doSummary=False)

        colCount = 1
        hex_key = "indx"
        csvPathname = SYNDATASETS_DIR + "/" + "indx.csv"
        write_syn_dataset(csvPathname, rowCount, colCount, SEEDPERFILE)

        parseResult = h2i.import_parse(path=csvPathname, schema='local', 
            hex_key=hex_key, timeoutSecs=3000, retryDelaySecs=2, doSummary=False)

        inspect = h2o_cmd.runInspect(key=hex_key)
        print "numRows:", inspect['numRows']
        print "numCols:", inspect['numCols']

        for trial in range(10):
            for execExpr in exprList:
                start = time.time()
                execResult, result = h2e.exec_expr(h2o.nodes[0], execExpr, resultKey=None, timeoutSecs=300)
                execTime = time.time() - start
                print 'exec took', execTime, 'seconds'

        h2o.check_sandbox_for_errors()



if __name__ == '__main__':
    h2o.unit_main()
