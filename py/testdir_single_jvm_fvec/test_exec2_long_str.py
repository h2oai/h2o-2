import unittest, random, sys, time
sys.path.extend(['.','..','py'])
import h2o, h2o_browse as h2b, h2o_exec as h2e, h2o_hosts, h2o_import as h2i


print "Throw in some unary ! in the expressions from test_exec2_operators2.py"

initList = [
        ('r0.hex', 'r0.hex=c(1.3,0,1,2,3,4,5)'),
        ('r1.hex', 'r1.hex=c(2.3,0,1,2,3,4,5)'),
        ('r2.hex', 'r2.hex=c(3.3,0,1,2,3,4,5)'),
        ('r3.hex', 'r3.hex=c(4.3,0,1,2,3,4,5)'),
        ('r4.hex', 'r4.hex=c(5.3,0,1,2,3,4,5)'),
        ('r', 'r=i.hex'),
        ('z.hex', 'z.hex=c(0)'),
        ]

exprListFull = [
        'r[,1]=r[,1]+r[,3];',
        'r[,1]=ifelse(r[,1],r[,2], ifelse(r[,1],r[,2],r[,3]));',
        'r[,1]=ifelse(r[,1],r[,2],ifelse(r[,1],r[,2],ifelse(r[,1],r[,2],ifelse(r[,1],r[,2],ifelse(r[,1],r[,2],r[,3])))));',
        ]

fail = [
        'r[,1]=\
            ifelse(r[,1],r[,2],\
            ifelse(r[,1],r[,2],\
            ifelse(r[,1],r[,2],\
            ifelse(r[,1],r[,2],\
            ifelse(r[,1],r[,2],r[,3])))));'
        ]


# incrementally concatenate
exprList = []
expr = ""
for i in range(10):
    expr += random.choice(exprListFull)
    exprList.append(expr)

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

    def test_exec2_operators2(self):
        bucket = 'home-0xdiag-datasets'
        csvPathname = 'standard/covtype.data'
        hexKey = 'i.hex'
        parseResult = h2i.import_parse(bucket=bucket, path=csvPathname, schema='put', hex_key=hexKey)

        for resultKey, execExpr in initList:
            h2e.exec_expr(h2o.nodes[0], execExpr, resultKey=resultKey, timeoutSecs=4)
        start = time.time()
        h2e.exec_expr_list_rand(len(h2o.nodes), exprList, None, maxTrials=200, timeoutSecs=10)

        # now run them just concatenating each time. We don't do any template substitutes, so don't need
        # exec_expr_list_rand()
        
        for execExpr in exprList:
            h2e.exec_expr(h2o.nodes[0], execExpr, resultKey=None, timeoutSecs=4)

        h2o.check_sandbox_for_errors()
        print "exec end on ", "operators" , 'took', time.time() - start, 'seconds'


if __name__ == '__main__':
    h2o.unit_main()
