import unittest, random, sys, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_browse as h2b, h2o_exec as h2e, h2o_import as h2i, h2o_cmd


print "exec: adding col that's not +1 of last col, causes assertion error"

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init(java_heap_GB=4)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_exec2_append_cols(self):
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

        for i in range(1,10):
            execExpr = 's.hex[,%s] = r.hex[,%s]' % (i, i),
            h2e.exec_expr(h2o.nodes[0], execExpr, resultKey='s.hex', timeoutSecs=10)

        inspect = h2o_cmd.runInspect(key='s.hex')
        # check the names on all cols is correct 
        cols = inspect['cols']
        print "cols:", h2o.dump_json(cols)
        for i,c in enumerate(cols):
            actual = c['name']
            expected = 'C' + str(i+1)
            self.assertEqual(actual, expected,
                msg="actual col name: %s expected col name %s" % (actual, expected))

        # make it fail with this one (skip)
        execExpr = 's.hex[,%s] = r.hex[,%s]' % (2, 1),
        h2e.exec_expr(h2o.nodes[0], execExpr, resultKey='s.hex', timeoutSecs=10)

        inspect = h2o_cmd.runInspect(key='s.hex')
        print "\ns.hex" \
            "    numRows:", "{:,}".format(inspect['numRows']), \
            "    numCols:", "{:,}".format(inspect['numCols'])
        

        h2o.check_sandbox_for_errors()


if __name__ == '__main__':
    h2o.unit_main()
