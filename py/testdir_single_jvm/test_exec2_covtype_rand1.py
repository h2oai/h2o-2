import unittest, random, sys, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_exec as h2e, h2o_import as h2i

zeroList = [
        'Result.hex = Result0 = c(0)',
        'Result.hex = Result = c(0)',
        ]
# randomBitVector
# randomFilter
# factor
# bug?
exprList = [
        # 'Result.hex = Result<n> = slice(c.hex[<col1>],<row>)',
        'Result<n> = c.hex[,<col1>] = ((c.hex[,2]==0))',
        'Result<n> = c.hex[,<col1>]',
        # 'Result<n> = min(c.hex[,<col1>], c.hex[,<col1>])',
        # 'Result<n> = max(c.hex[,<col1>], c.hex[,<col1>]) + Result.hex[,0]',
        ### 'Result.hex = Result<n> = mean(c.hex[,<col1>]) + Result.hex[0]',
        # 'Result<n> = sum(c.hex[,<col1>], c.hex[,<col1>]) + Result.hex[,0]',
        # have to figure out how to avoid infinity results
        # 'Result<n> = log(c.hex[<col1>]) + Result.hex[0]',
        ]

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init(1)

    @classmethod
    def tearDownClass(cls):
        # wait while I inspect things
        # time.sleep(1500)
        h2o.tear_down_cloud()

    def test_exec2_covtype_rand1(self):
        csvPathname = 'standard/covtype.data'
        parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, schema='local', hex_key='c.hex', timeoutSecs=15)
        print "\nParse key is:", parseResult['destination_key']

        ### h2b.browseTheCloud()
        h2e.exec_zero_list(zeroList)
        start = time.time()
        h2e.exec_expr_list_rand(len(h2o.nodes), exprList, 'c.hex', 
            maxCol=54, maxRow=400000, maxTrials=200, timeoutSecs=10)

        h2o.check_sandbox_for_errors()
        print "exec end on ", "covtype.data" , 'took', time.time() - start, 'seconds'


if __name__ == '__main__':
    h2o.unit_main()
