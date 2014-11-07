import unittest, sys, random, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_exec as h2e, h2o_import as h2i

zeroList = [
        'Result9 = c.hex',
        'Result8 = c.hex',
        'Result7 = c.hex',
        'Result6 = c.hex',
        'Result5 = c.hex',
        'Result4 = c.hex',
        'Result3 = c.hex',
        'Result2 = c.hex',
        'Result1 = c.hex',
        'Result0 = c.hex',
        'Result = c.hex',
]

# randomBitVector
# randomFilter
# log
# makeEnum
# bug?
#        ['Result<n> = slice(c.hex[<col1>],<row>)',
exprList = [
        'Result<n>[,1] = (c.hex[,2]==0) ? 54321 : 54321',
        'Result<n> = c.hex[,<col1>]',
        # assigning a scalar to an existing key, makes it get
        # treated like a scalar and disappear from visibility by the next exec
        # 'Result<n> = min(c.hex[,<col1>])',
        # 'Result<n> = max(c.hex[,<col1>]) + Result[,1]',
        # 'Result<n> = sum(c.hex[,<col1>]) + Result[,1]',
    ]

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init(2, java_heap_GB=5)

    @classmethod
    def tearDownClass(cls):
        # wait while I inspect things
        # time.sleep(1500)
        h2o.tear_down_cloud()

    def test_loop_random_exec_covtype(self):
        csvPathname = 'standard/covtype.data'
        parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, schema='put', hex_key='c.hex', timeoutSecs=15)
        print "\nParse key is:", parseResult['destination_key']

        # h2b.browseTheCloud()
        h2e.exec_zero_list(zeroList)
        start = time.time()
        h2e.exec_expr_list_rand(len(h2o.nodes), exprList, 'c.hex',
            maxCol=54, maxRow=400000, maxTrials=200, timeoutSecs=15)
        h2o.check_sandbox_for_errors()
        print "exec end on ", "covtype.data" , 'took', time.time() - start, 'seconds'


if __name__ == '__main__':
    h2o.unit_main()
