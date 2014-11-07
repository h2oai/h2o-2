import unittest, random, sys, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_browse as h2b, h2o_exec as h2e, h2o_import as h2i


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

# make it incrementally longer, so we can see where it fails. 
exprList = []

# try it with simple values and with a key column
base1 = 'r[,1]='
base2 = 'a='
for i in range(1,20):
    # put i in there just so we don't have to count
    expr1 = "c(%s); " % i + base1
    expr2 = "a=c(%s);b=c(%s);d=c(%s); " % (i,i,i) + base2
    for j in range(i):
        expr1 += 'ifelse(r[,1],r[,2], '
        expr2 += 'ifelse(a,b, '
    # last else
    expr1 += "r[,1]"
    expr2 += "d"
    # now add the close parens
    for j in range(i):
        expr1 += ")"
        expr2 += ")"
    
    # shouldn't be necessary
    expr1 += ";"
    expr2 += ";"
    exprList.append(expr1)
    exprList.append(expr2)

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init(1)

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

        for execExpr in exprList:
            h2e.exec_expr(h2o.nodes[0], execExpr, resultKey=None, timeoutSecs=4)

        h2o.check_sandbox_for_errors()
        print "exec end on ", "operators" , 'took', time.time() - start, 'seconds'


if __name__ == '__main__':
    h2o.unit_main()
