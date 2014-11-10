import unittest, random, sys, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_browse as h2b, h2o_exec as h2e, h2o_import as h2i, h2o_cmd

initList = [
        ('r', 'r=i.hex'),
        ]

exprList = [
    "a=c(1,2,3); a = r[r[,1]>2,]",
    "a=c(1,2,3); a = r[r[,1]>10,]",
    ]

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_NOPASS_exec2_empty_result(self):
        bucket = 'smalldata'
        csvPathname = 'iris/iris2.csv'
        hexKey = 'i.hex'
        parseResult = h2i.import_parse(bucket=bucket, path=csvPathname, schema='put', hex_key=hexKey)

        for resultKey, execExpr in initList:
            h2e.exec_expr(h2o.nodes[0], execExpr, resultKey=None, timeoutSecs=10)

        start = time.time()
        for execExpr in exprList:
            h2e.exec_expr(h2o.nodes[0], execExpr, resultKey=None, timeoutSecs=10)
            rSummary = h2o_cmd.runSummary(key="a")
            h2o_cmd.infoFromSummary(rSummary)

            
        h2o.check_sandbox_for_errors()
        print "exec end on ", "operators" , 'took', time.time() - start, 'seconds'


if __name__ == '__main__':
    h2o.unit_main()
