import unittest, random, sys, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_browse as h2b, h2o_exec as h2e, h2o_import as h2i

DO_UNIMPLEMENTED = False
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

    def test_exec2_env_fail(self):
        bucket = 'smalldata'
        csvPathname = 'iris/iris2.csv'
        hexKey = 'i.hex'
        parseResult = h2i.import_parse(bucket=bucket, path=csvPathname, schema='put', hex_key=hexKey)

        if DO_UNIMPLEMENTED:
            execExpr = "mean2=function(x){apply(x,2,sum)/nrow(x)};mean2(i.hex)"
        else:
            execExpr = "mean2=function(x){apply(x,1,sum)/nrow(x)};mean2(i.hex)"

        start = time.time()
        h2e.exec_expr(execExpr=execExpr, timeoutSecs=30) # unneeded but interesting
        print "exec end on ", "operators" , 'took', time.time() - start, 'seconds'
        h2o.check_sandbox_for_errors()


if __name__ == '__main__':
    h2o.unit_main()
