import unittest, random, sys, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i, h2o_exec as h2e

exprList = [
    # "rTest=randomFilter(<keyX>,58101,12345)",
    "a=runif(c.hex[,1], -1); rTrain=<keyX>[a<0.8,]",
    # doesn't work yet
    # "r2=c.hex[1:100,]",
    ]

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init(1,java_heap_GB=1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_exec2_filter_slice(self):
        timeoutSecs = 10
        csvFilename = "covtype.data"
        csvPathname = 'standard/covtype.data'
        hex_key = "c.hex"
        parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, schema='put', hex_key=hex_key, 
            timeoutSecs=20)
        print "Parse result['desination_key']:", parseResult['destination_key']
        inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])

        for trial in range(10):
            print "Doing the execs in order, to feed filters into slices"
            nodeX = 0
            for exprTemplate in exprList:
                execExpr = h2e.fill_in_expr_template(exprTemplate, colX=0, n=0, row=1, keyX=hex_key, m=2)
                execResultInspect, min_value = h2e.exec_expr(h2o.nodes[nodeX], execExpr, 
                    resultKey=None, timeoutSecs=10)

                print "min_value:", min_value, "execExpr:", execExpr
                h2o.verboseprint("min: ", min_value, "trial:", trial)

if __name__ == '__main__':
    h2o.unit_main()
