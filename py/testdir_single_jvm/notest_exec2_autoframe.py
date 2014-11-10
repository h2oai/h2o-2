import unittest, random, sys, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_exec as h2e, h2o_import as h2i

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
        # wait while I inspect things
        # time.sleep(1500)
        h2o.tear_down_cloud()

    def test_exec2_autoframe(self):
        csvPathname = 'standard/covtype.data'
        parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, schema='put', 
            hex_key='c.hex', timeoutSecs=10, doSummary=False)
        print "\nParse key is:", parseResult['destination_key']

        ### h2b.browseTheCloud()
        start = time.time()
        # passes with suffix, fails without?
        # suffix = ""
        suffix = ".hex"
        for i in range(54):
            # try the funky c(6) thing like  R, instead of just 6
            execExpr = "Result" + str(i) + suffix + " = c.hex[,c(" + str(i+1) + ")]"
            print "execExpr:", execExpr
            h2e.exec_expr(h2o.nodes[0], execExpr, resultKey="Result" + str(i) + suffix, 
                timeoutSecs=4)

        h2o.check_sandbox_for_errors()
        print "exec end on ", "covtype.data" , 'took', time.time() - start, 'seconds'


if __name__ == '__main__':
    h2o.unit_main()
