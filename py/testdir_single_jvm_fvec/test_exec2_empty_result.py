import unittest, random, sys, time
sys.path.extend(['.','..','py'])
import h2o, h2o_browse as h2b, h2o_exec as h2e, h2o_hosts, h2o_import as h2i

initList = [
        ('r', 'r=i.hex'),
        ]

exprListFull = [
    "r[r[,1]>2,1]",
    "r[r[,1]>10,1]",
    "a=c(1,2,3); a[a[,1]>2,1]",
    "a=c(1,2,3); a[a[,1]>10,1]",
    ]

# concatenate a lot of random choices to make life harder
if 1==0:
    exprList = []
    for i in range(10):
        expr = ""
        for j in range(1):
            expr += "z.hex=" + random.choice(exprListFull) + ";"
            # expr += random.choice(exprListFull) + ";"
        exprList.append(expr)

exprList = exprListFull

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED, localhost
        SEED = h2o.setup_random_seed()
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(1, java_heap_GB=14)
        else:
            h2o_hosts.build_cloud_with_hosts(1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_exec2_empty_result(self):
        h2o.beta_features = True
        bucket = 'home-0xdiag-datasets'
        # csvPathname = 'airlines/year2013.csv'
        csvPathname = 'standard/covtype.data'
        hexKey = 'i.hex'
        parseResult = h2i.import_parse(bucket=bucket, path=csvPathname, schema='put', hex_key=hexKey)

        for resultKey, execExpr in initList:
            h2e.exec_expr(h2o.nodes[0], execExpr, resultKey=None, timeoutSecs=10)
        start = time.time()
        # h2e.exec_expr_list_rand(len(h2o.nodes), exprList, 'r1.hex', maxTrials=200, timeoutSecs=10)
        for execExpr in exprList:
            h2e.exec_expr(h2o.nodes[0], execExpr, resultKey=None, timeoutSecs=10)

        h2o.check_sandbox_for_errors()
        print "exec end on ", "operators" , 'took', time.time() - start, 'seconds'


if __name__ == '__main__':
    h2o.unit_main()
