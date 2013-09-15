import unittest, sys, random, time
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_exec as h2e, h2o_hosts, h2o_import as h2i

zeroList = [
        'Result0 = 0',
        'Result = 0',
]

# randomBitVector
# randomFilter
# log
# makeEnum
# bug?
#        ['Result<n> = slice(c.hex[<col1>],<row>)',
exprList = [
        'Result<n> = colSwap(c.hex,<col1>,(c.hex[2]==0 ? 54321 : 54321))',
        'Result<n> = c.hex[<col1>]',
        'Result<n> = min(c.hex[<col1>])',
        'Result<n> = max(c.hex[<col1>]) + Result[0]',
        'Result<n> = mean(c.hex[<col1>]) + Result[0]',
        'Result<n> = sum(c.hex[<col1>]) + Result[0]',
    ]

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED, localhost
        SEED = h2o.setup_random_seed()
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(2, java_heap_GB=5)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        # wait while I inspect things
        # time.sleep(1500)
        h2o.tear_down_cloud()

    def test_loop_random_exec_covtype(self):
        csvPathname = 'UCI/UCI-large/covtype/covtype.data'
        parseResult = h2i.import_parse(bucket='datasets', path=csvPathname, schema='put', hex_key='c.hex', timeoutSecs=15)
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
