import unittest, random, sys, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_browse as h2b, h2o_exec as h2e, h2o_import as h2i

initList = [
        ('r.hex', 'r.hex=i.hex'),
        ('r1.hex', 'r1.hex=i.hex'),
        ('r2.hex', 'r2.hex=i.hex'),
        ('r3.hex', 'r3.hex=i.hex'),
        ('x', 'x=c(1)'),
        ('y', 'y=c(1)'),
        ]

exprList = [
    #  java.lang.IllegalArgumentException: Only one column-of-columns for column selection
    # "ddply(r.hex,r.hex,sum)",
    # java.lang.IllegalArgumentException: Too many columns selected
    # "ddply(r.hex,seq_len(10000),sum)",
    # java.lang.IllegalArgumentException: NA not a valid column
    # "ddply(r.hex,NA,sum)",
    # "ddply(r.hex,c(1,NA,3),sum)",

    # skip col 1? too slow? (altitude)
    # "a.hex = ddply(r.hex,c(1),sum)",
    "a.hex = ddply(r.hex,c(2),sum)",
    "a.hex = ddply(r.hex,c(3),sum)",
    "a.hex = ddply(r.hex,c(4),sum)",
    ]

for i in range(1,55):
    exprList.append("a.hex = ddply(r.hex, c(%s), sum)" % i)

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init(1, java_heap_GB=14)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_exec2_ddply_multi(self):
        bucket = 'home-0xdiag-datasets'
        # csvPathname = 'airlines/year2013.csv'
        csvPathname = 'standard/covtype.data'
        hexKey = 'i.hex'
        parseResult = h2i.import_parse(bucket=bucket, path=csvPathname, schema='put', hex_key=hexKey)

        for resultKey, execExpr in initList:
            h2e.exec_expr(h2o.nodes[0], execExpr, resultKey=None, timeoutSecs=60)
        # h2e.exec_expr_list_rand(len(h2o.nodes), exprList, 'r1.hex', maxTrials=200, timeoutSecs=10)
        for execExpr in exprList:
            start = time.time()
            (resultExec, result) = h2e.exec_expr(h2o.nodes[0], execExpr, resultKey=None, timeoutSecs=400)
            print "exec end took", time.time() - start, "seconds"
            print "result:", result
            print "resultExec:", h2o.dump_json(resultExec)

        h2o.check_sandbox_for_errors()


if __name__ == '__main__':
    h2o.unit_main()
