import unittest, random, sys, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_browse as h2b, h2o_exec as h2e, h2o_import as h2i, h2o_cmd

initList = [
        ('r.hex', 'r.hex=i.hex'),
        ]

# quantile lists of 10, 100)
exprList = []

# try 3 cols
for col in (1,4,10):
    for num in (1,10,100):
        cList = [1/(i+0.0) for i in range(1,num+1)]
        expr = "a.hex = quantile(r.hex[,%s],c(%s))" % (col, ','.join(map(str,cList)) )
        exprList.append( (expr, num) )

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init(1, java_heap_GB=14)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_exec2_operators(self):
        bucket = 'home-0xdiag-datasets'
        # csvPathname = 'airlines/year2013.csv'
        csvPathname = 'standard/covtype.data'
        hexKey = 'i.hex'
        parseResult = h2i.import_parse(bucket=bucket, path=csvPathname, schema='put', hex_key=hexKey)

        for resultKey, execExpr in initList:
            h2e.exec_expr(h2o.nodes[0], execExpr, resultKey=None, timeoutSecs=10)
        # h2e.exec_expr_list_rand(len(h2o.nodes), exprList, 'r1.hex', maxTrials=200, timeoutSecs=10)
        for (execExpr, num) in exprList:
            start = time.time()
            resultExec, result = h2e.exec_expr(h2o.nodes[0], execExpr, resultKey=None, timeoutSecs=180)
            print h2o.dump_json(resultExec)
            print 'exec end took', time.time() - start, 'seconds'

            inspect = h2o_cmd.runInspect(key='a.hex')
            numCols = inspect['numCols']
            numRows = inspect['numRows']
            print "numCols:", numCols
            print "numRows:", numRows
            self.assertEqual(numCols, 1)
            self.assertEqual(numRows, num)

            h2o.check_sandbox_for_errors()


if __name__ == '__main__':
    h2o.unit_main()
