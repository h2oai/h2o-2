import unittest
import random, sys, time
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_browse as h2b, h2o_exec as h2e, h2o_hosts
initList = [
        'Result0 = 0',
        'Result1 = 1',
        'Result2 = 2',
        'Result3 = 3',
        ]

exprList = [
        'Result<n> = Result0 * Result<n-1>',
        'Result<n> = Result1 + Result<n-1>',
        'Result<n> = Result2 / Result<n-1>',
        'Result<n> = Result3 - Result<n-1>',
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
        h2o.tear_down_cloud()

    def test_exec_operators(self):
        if 1==1:
            for execExpr in initList:
                h2e.exec_expr(h2o.nodes[0], execExpr, resultKey="Result.hex", timeoutSecs=4)
        else:
            # init with put_value
            for i in range(0,5):
                key = "ResultUnparsed" + str(i)
                put = h2o.nodes[0].put_value(i, key=key, repl=None)
                # have to parse the key after you put_value it. put_value should parse the result first!
                key2 = "Result" + str(i) 
                parse = h2o.nodes[0].parse(put['key'], key2, timeoutSecs=10)

        start = time.time()
        h2e.exec_expr_list_rand(len(h2o.nodes), exprList, None, maxTrials=200, timeoutSecs=10)

        h2o.check_sandbox_for_errors()
        print "exec end on ", "operators" , 'took', time.time() - start, 'seconds'


if __name__ == '__main__':
    h2o.unit_main()
