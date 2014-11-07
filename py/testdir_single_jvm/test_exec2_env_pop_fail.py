import unittest, random, sys, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_browse as h2b, h2o_exec as h2e, h2o_import as h2i

DO_PARSE=True
DO_FAIL=True
exprList = [
        "z.hex=0;",

        "r.hex=i.hex",

        # "r1.hex=i.hex",
        # "r2.hex=i.hex",
        # "r3.hex=i.hex",
        # "z.hex=1.23 >=2.34;",
        # "z.hex=1.23 >=2.34;",

        # "z.hex=is.na(i.hex);",
        "z.hex=i.hex;",

        # "z.hex=is.na(i.hex);",
        # "z.hex=i.hex",
        # "z.hex=1.23 >=2.34;",

        # temp
        "z.hex=0;",
        ]

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
        # h2o.sleep(3600)
        h2o.tear_down_cloud()

    def test_exec2_env_pop_fail(self):
        h2b.browseTheCloud()

        if DO_FAIL:
            bucket = 'home-0xdiag-datasets'
            csvPathname = 'airlines/year2013.csv'
        else:
            bucket = 'smalldata'
            csvPathname = 'iris/iris2.csv'
        hexKey = 'i.hex'

        parseResult = h2i.import_parse(bucket=bucket, path=csvPathname, schema='put', hex_key=hexKey)
        for execExpr in exprList:
            h2e.exec_expr(h2o.nodes[0], execExpr, resultKey=None, timeoutSecs=4)
            h2o.check_sandbox_for_errors()

        # print "Sleeping"
        # h2o.sleep(3600)


if __name__ == '__main__':
    h2o.unit_main()
