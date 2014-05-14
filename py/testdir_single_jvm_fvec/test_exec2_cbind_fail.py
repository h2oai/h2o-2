import unittest, random, sys, time
sys.path.extend(['.','..','py'])
import h2o, h2o_exec as h2e, h2o_hosts, h2o_import as h2i

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

    def test_exec2_cbind_fail1(self):

        for i in range(5):
            execExpr = "h <- cbind(c(0,0,0), c(1,1,1))"
            h2e.exec_expr(execExpr=execExpr, timeoutSecs=30)
            # have to make sure they're created as keys for reuse between execs
            execExpr = "a=c(0,0,0); b=c(0,0,0); d=c(0,0,0); e=c(0,0,0); f=c(0,0,0); g= c(0,0,0);"
            h2e.exec_expr(execExpr=execExpr, timeoutSecs=30)
            execExpr = "b=a; d=a; f=a; g=a;"
            h2e.exec_expr(execExpr=execExpr, timeoutSecs=30)
            execExpr = "h <- cbind(a, b)"
            h2e.exec_expr(execExpr=execExpr, timeoutSecs=30)
            execExpr = "h <- cbind(a, b, d)"
            h2e.exec_expr(execExpr=execExpr, timeoutSecs=30)
            execExpr = "h <- cbind(a, b, d, e)"
            h2e.exec_expr(execExpr=execExpr, timeoutSecs=30)
            execExpr = "h <- cbind(a, b, d, e, f)"
            h2e.exec_expr(execExpr=execExpr, timeoutSecs=30)
            execExpr = "h <- cbind(a, b, d, e, f, g)"
            h2e.exec_expr(execExpr=execExpr, timeoutSecs=30)

        h2o.check_sandbox_for_errors()

    def test_exec2_cbind_fail2(self):

        for i in range(5):
            execExpr = "b=c(0,0,0,0,0,0,0,0,0,0,0,0)"
            h2e.exec_expr(execExpr=execExpr, timeoutSecs=30)
            # have to make sure they're created as keys for reuse between execs
            execExpr = "a=b"
            h2e.exec_expr(execExpr=execExpr, timeoutSecs=30)
            execExpr = "d=b"
            h2e.exec_expr(execExpr=execExpr, timeoutSecs=30)
            execExpr = "cbind(a,b,d)"
            h2e.exec_expr(execExpr=execExpr, timeoutSecs=30)

        h2o.check_sandbox_for_errors()


if __name__ == '__main__':
    h2o.unit_main()
