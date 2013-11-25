import unittest, random, sys, time
sys.path.extend(['.','..','py'])

import h2o, h2o_browse as h2b, h2o_exec as h2e, h2o_hosts
initList = [
        ('Result0.hex', 'Result0.hex=c(1,0,1,2,3,4,5)'),
        ('Result1.hex', 'Result1.hex=c(2,0,1,2,3,4,5)'),
        ('Result2.hex', 'Result2.hex=c(3,0,1,2,3,4,5)'),
        ('Result3.hex', 'Result3.hex=c(4,0,1,2,3,4,5)'),
        ('Result4.hex', 'Result4.hex=c(5,0,1,2,3,4,5)'),
        ]

exprList = [
        # 'Result<n>[,0] = Result0[,0] * Result<n-1>[,0]',
        # 'Result<n>[0,] = Result1[0,] + Result<n-1>[0,]',
        # 'Result<n> = Result1 + Result<n-1>',
        'Result0.hex[,1]=Result1.hex[,1]',
        'Result0.hex[,1]=Result0.hex[,1]',
        'Result1.hex[,1]=Result1.hex[,1]',
        'Result2.hex[,1]=Result2.hex[,1]',
        'Result3.hex[,1]=Result3.hex[,1]',
        # 'Result<n>.hex=min(Result0.hex,1+2)',
        # 'Result<n>.hex=Result1.hex + 1',
        # 'Result<n>.hex=Result2.hex + 1',
        # 'Result<n>.hex=Result3.hex + 1',
        # 'Result<n>[,0] = Result2[,0] / Result<n-1>[,0]',
        # 'Result<n>[,0] = Result3[,0] - Result<n-1>[,0]',
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
        for resultKey, execExpr in initList:
            h2e.exec_expr(h2o.nodes[0], execExpr, resultKey=resultKey, timeoutSecs=4)
        start = time.time()
        h2e.exec_expr_list_rand(len(h2o.nodes), exprList, None, maxTrials=200, timeoutSecs=10)

        h2o.check_sandbox_for_errors()
        print "exec end on ", "operators" , 'took', time.time() - start, 'seconds'


if __name__ == '__main__':
    h2o.unit_main()
