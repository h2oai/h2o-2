import unittest, random, sys, time
sys.path.extend(['.','..','py'])
import h2o, h2o_browse as h2b, h2o_exec as h2e, h2o_hosts, h2o_import as h2i, h2o_cmd


print "exec: adding col that's not +1 of last col, causes assertion error"

exprList = [
    's.hex = r.hex[,1]',
    's.hex[,2] = r.hex[,2]',
    's.hex[,3] = r.hex[,3]',
    's.hex[,4] = r.hex[,4]',
    's.hex[,5] = r.hex[,5]',
    's.hex[,6] = r.hex[,6]',
    's.hex[,7] = r.hex[,7]',
    's.hex[,8] = r.hex[,8]',
    's.hex[,9] = r.hex[,9]',
    's.hex[,11] = r.hex[,11]',
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
            h2o.build_cloud(1, java_heap_GB=14)
        else:
            h2o_hosts.build_cloud_with_hosts(1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_exec2_na_chop(self):
        h2o.beta_features = True
        bucket = 'home-0xdiag-datasets'
        csvPathname = 'standard/covtype.data'
        hexKey = 'r.hex'
        parseResult = h2i.import_parse(bucket=bucket, path=csvPathname, schema='put', hex_key=hexKey)
        inspect = h2o_cmd.runInspect(key='r.hex')
        print "\nr.hex" \
            "    numRows:", "{:,}".format(inspect['numRows']), \
            "    numCols:", "{:,}".format(inspect['numCols'])
        numRows = inspect['numRows']
        numCols = inspect['numCols']

        execExpr = 's.hex = r.hex[,1]',
        h2e.exec_expr(h2o.nodes[0], execExpr, resultKey='s.hex', timeoutSecs=10)

        # for i in range(1,56):
        for i in range(1,10):
            execExpr = 's.hex[,%s] = r.hex[,%s]' % (i, i),
            h2e.exec_expr(h2o.nodes[0], execExpr, resultKey='s.hex', timeoutSecs=10)

        # make it fail with this one (skip)
        execExpr = 's.hex[,%s] = r.hex[,%s]' % (101, 1),
        h2e.exec_expr(h2o.nodes[0], execExpr, resultKey='s.hex', timeoutSecs=10)

        inspect = h2o_cmd.runInspect(key='s.hex')
        print "\ns.hex" \
            "    numRows:", "{:,}".format(inspect['numRows']), \
            "    numCols:", "{:,}".format(inspect['numCols'])

        h2o.check_sandbox_for_errors()


if __name__ == '__main__':
    h2o.unit_main()
