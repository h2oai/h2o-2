import unittest, random, sys, time
sys.path.extend(['.','..','py'])
import h2o, h2o_browse as h2b, h2o_exec as h2e, h2o_hosts, h2o_import as h2i, h2o_cmd

# quantile lists of 10, 100)
exprList = []


print "Check case of all NA in col for quantile. Also a single value"
print "FIX! probably should return NA if all NA"
print "What is the right answer for single values? I guess that value, should check that."
print "probably should check the enum case here too"
# try 3 cols
exprList = [


    # two col case
    ("nah = cbind(c(NA,NA,NA), c(0,0,0)); abc = quantile(nah[,1], c(1))", 1),
    ("nah = cbind(c(NA,NA,NA), c(0,0,0)); abc = quantile(nah[,1], c(0.4))", 1),
    ("nah = cbind(c(NA,NA,NA), c(0,0,0)); abc = quantile(nah[,1], c(0.5))", 1),
    ("nah = cbind(c(NA,NA,NA), c(0,0,0)); abc = quantile(nah[,1], c(0.6))", 1),
    ("nah = cbind(c(NA,NA,NA), c(0,0,0)); abc = quantile(nah[,1], c(0))", 1),

    # error, data should be first param
    ("nah = c(0,0,0); abc = quantile(c(0), nah)", 3),
    ("nah = c(0,0,0); abc = quantile(c(0.4), nah)", 3),
    ("nah = c(0,0,0); abc = quantile(c(0.5), nah)", 3),
    ("nah = c(0,0,0); abc = quantile(c(0.6), nah)", 3),
    ("nah = c(0,0,0); abc = quantile(c(1), nah)", 3),

    ("nah = c(NA,NA,NA); abc = quantile(nah, c(1))", 1),
    ("nah = c(NA,NA,NA); abc = quantile(nah, c(0.4))", 1),
    ("nah = c(NA,NA,NA); abc = quantile(nah, c(0.5))", 1),
    ("nah = c(NA,NA,NA); abc = quantile(nah, c(0.6))", 1),
    ("nah = c(NA,NA,NA); abc = quantile(nah, c(0))", 1),

    # error, data should be first param
    ("nah = c(0,0); abc = quantile(c(0), nah)", 3),
    ("nah = c(0,0); abc = quantile(c(0.4), nah)", 3),
    ("nah = c(0,0); abc = quantile(c(0.5), nah)", 3),
    ("nah = c(0,0); abc = quantile(c(0.6), nah)", 3),
    ("nah = c(0,0); abc = quantile(c(1), nah)", 3),

    ("nah = c(NA,NA); abc = quantile(nah, c(1))", 1),
    ("nah = c(NA,NA); abc = quantile(nah, c(0.4))", 1),
    ("nah = c(NA,NA); abc = quantile(nah, c(0.5))", 1),
    ("nah = c(NA,NA); abc = quantile(nah, c(0.6))", 1),
    ("nah = c(NA,NA); abc = quantile(nah, c(0))", 1),

    # error, data should be first param
    ("nah = c(0); abc = quantile(c(0), nah)", 3),
    ("nah = c(0); abc = quantile(c(0.4), nah)", 3),
    ("nah = c(0); abc = quantile(c(0.5), nah)", 3),
    ("nah = c(0); abc = quantile(c(0.6), nah)", 3),
    ("nah = c(0); abc = quantile(c(1), nah)", 3),

    ("nah = c(NA); abc = quantile(nah, c(1))", 1),
    ("nah = c(NA); abc = quantile(nah, c(0.4))", 1),
    ("nah = c(NA); abc = quantile(nah, c(0.5))", 1),
    ("nah = c(NA); abc = quantile(nah, c(0.6))", 1),
    ("nah = c(NA); abc = quantile(nah, c(0))", 1),

    # have to figure out how to test the expected error here
    # ('nah = c("N","N","N"); abc = quantile(nah, c(1))', 1),
    # ('nah = c("N","N","N"); abc = quantile(nah, c(0.4))', 1),
    # ('nah = c("N","N","N"); abc = quantile(nah, c(0.5))', 1),
    # ('nah = c("N","N","N"); abc = quantile(nah, c(0.6))', 1),
    # ('nah = c("N","N","N"); abc = quantile(nah, c(0))', 1),

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
            h2o.build_cloud(1, java_heap_GB=1)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_exec2_quantile_na_scalar(self):
        h2o.beta_features = True

        for (execExpr, num) in exprList:
            start = time.time()
            resultExec, result = h2e.exec_expr(h2o.nodes[0], execExpr, resultKey=None, timeoutSecs=180)
            print h2o.dump_json(resultExec)
            print 'exec end took', time.time() - start, 'seconds'

            inspect = h2o_cmd.runInspect(key='abc')
            numCols = inspect['numCols']
            numRows = inspect['numRows']
            print "numCols:", numCols
            print "numRows:", numRows
            self.assertEqual(numCols, 1)
            self.assertEqual(numRows, num)

            h2o.check_sandbox_for_errors()


if __name__ == '__main__':
    h2o.unit_main()
