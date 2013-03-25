import unittest
import random, sys, time
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_browse as h2b, h2o_exec as h2e, h2o_hosts

zeroList = [
        'Result0 = 0',
        'Result = 0',
        ]
# randomBitVector
# randomFilter
# factor
# bug?
exprList = [
        'Result<n> = slice(c.hex[<col1>],<row>)',
        'Result<n> = colSwap(c.hex,<col1>,(c.hex[2]==0 ? 54321 : 54321))',
        'Result<n> = c.hex[<col1>]',
        'Result<n> = min(c.hex[<col1>])',
        'Result<n> = max(c.hex[<col1>]) + Result.hex[0]',
        'Result<n> = mean(c.hex[<col1>]) + Result.hex[0]',
        'Result<n> = sum(c.hex[<col1>]) + Result.hex[0]',
        'Result<n> = log(c.hex[<col1>]) + Result.hex[0]',
        ]

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        # for determinism, I guess we should spit out the seed?
        # random.seed(SEED)
        SEED = random.randint(0, sys.maxint)
        # if you have to force to redo a test
        # SEED = 
        random.seed(SEED)
        print "\nUsing random seed:", SEED

        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(1)
        else:
            h2o_hosts.build_cloud_with_hosts(1)

    @classmethod
    def tearDownClass(cls):
        # wait while I inspect things
        # time.sleep(1500)
        h2o.tear_down_cloud()

    def test_loop_random_exec_covtype(self):
        csvPathname = h2o.find_dataset('UCI/UCI-large/covtype/covtype.data')
        parseKey = h2o_cmd.parseFile(None, csvPathname, 'covtype.data', 'c.hex', 15)
        print "\nParse key is:", parseKey['destination_key']

        h2b.browseTheCloud()
        h2e.exec_zero_list(zeroList)
        start = time.time()
        h2e.exec_expr_list_rand(len(h2o.nodes), exprList, 'c.hex', 
            maxCol=54, maxRow=400000, maxTrials=200, timeoutSecs=10)

        h2o.check_sandbox_for_errors()
        print "exec end on ", "covtype.data" , 'took', time.time() - start, 'seconds'


if __name__ == '__main__':
    h2o.unit_main()
