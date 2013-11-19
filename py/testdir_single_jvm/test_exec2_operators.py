import unittest, random, sys, time
sys.path.extend(['.','..','py'])

import h2o, h2o_browse as h2b, h2o_exec as h2e, h2o_hosts

print "FIX! evidently visibility between expressions depends on the type. constants disappear?"
print "hack by creating vectors"
initList = [
        'Result0 = c(0)',
        'Result1 = c(1)',
        'Result2 = c(2)',
        'Result3 = c(3)',
        ]

# double assign to Result.hex, so the checker doesn't have different names to check?
exprList = [
        # 'Result.hex = Result<n> = Result0 * Result<n-1>',
        'Result.hex = Result<n> = Result1 + Result<n-1>',
        # 'Result.hex = Result<n> = Result2 / Result<n-1>',
        # 'Result.hex = Result<n> = Result3 - Result<n-1>',
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
            h2o.build_cloud(1)
        else:
            h2o_hosts.build_cloud_with_hosts(1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_exec_operators(self):
        h2o.beta_features = True

        for i, execExpr in enumerate(initList):
            if h2o.beta_features: # no default result
                resultKey = "Result" + str(i)
            else:
                resultKey = "Result.hex"
            h2e.exec_expr(h2o.nodes[0], execExpr, resultKey=resultKey, timeoutSecs=4)

        start = time.time()
        h2e.exec_expr_list_rand(len(h2o.nodes), exprList, None, maxTrials=200, timeoutSecs=10)

        h2o.check_sandbox_for_errors()
        print "exec end on ", "operators" , 'took', time.time() - start, 'seconds'


if __name__ == '__main__':
    h2o.unit_main()
