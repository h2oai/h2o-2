import unittest, random, sys, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_browse as h2b, h2o_exec as h2e, h2o_import as h2i, h2o_cmd

initList = [
        ('r.hex', 'r.hex=i.hex'),
        ]

exprList = [
    "s.hex = r.hex[!is.na(r.hex[,<col1>]),]"
]


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

    def test_exec2_na_chop(self):
        bucket = 'home-0xdiag-datasets'
        csvPathname = 'airlines/year2013.csv'
        hexKey = 'i.hex'
        parseResult = h2i.import_parse(bucket=bucket, path=csvPathname, schema='put', hex_key=hexKey)
        inspect = h2o_cmd.runInspect(key='i.hex')
        print "\nr.hex" \
            "    numRows:", "{:,}".format(inspect['numRows']), \
            "    numCols:", "{:,}".format(inspect['numCols'])
        numRows1 = inspect['numRows']
        numCols = inspect['numCols']

        for resultKey, execExpr in initList:
            h2e.exec_expr(h2o.nodes[0], execExpr, resultKey=None, timeoutSecs=10)
        start = time.time()
        h2e.exec_expr_list_rand(len(h2o.nodes), exprList, keyX='s.hex', maxTrials=200, timeoutSecs=30, maxCol=numCols-1)

        inspect = h2o_cmd.runInspect(key='s.hex')
        print "\ns.hex" \
            "    numRows:", "{:,}".format(inspect['numRows']), \
            "    numCols:", "{:,}".format(inspect['numCols'])
        numRows2 = inspect['numRows']

        print numRows1, numRows2


        h2o.check_sandbox_for_errors()
        print "exec end on ", "operators" , 'took', time.time() - start, 'seconds'


if __name__ == '__main__':
    h2o.unit_main()
