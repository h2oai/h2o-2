import unittest
import random, sys, time
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import as h2i, h2o_exec as h2e

exprList = [
    # "rTest=randomFilter(<keyX>,58101,12345)",
    "rTrain=randomFilter(<keyX>,522909,12345)",
    "r1=slice(rTrain,1)", # no third arg
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
            h2o.build_cloud(1,java_heap_GB=1)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_exec_filter_slice2(self):
        timeoutSecs = 10
        csvFilename = "covtype.data"
        csvPathname = h2o.find_dataset('UCI/UCI-large/covtype/covtype.data')
        key2 = "c"
        parseKey = h2o_cmd.parseFile(None, csvPathname, 'covtype.data', 'c', 10)
        print csvFilename, 'parse time:', parseKey['response']['time']
        print "Parse result['desination_key']:", parseKey['destination_key']
        inspect = h2o_cmd.runInspect(None, parseKey['destination_key'])

        for trial in range(10):
            print "Doing the execs in order, to feed filters into slices"
            nodeX = 0
            for exprTemplate in exprList:
                execExpr = h2e.fill_in_expr_template(exprTemplate, colX=0, n=0, row=1, key2=key2, m=2)
                time.sleep(2)
                h2o.check_sandbox_for_errors()

                execResultInspect, min_value = h2e.exec_expr(h2o.nodes[nodeX], execExpr, 
                    resultKey="Result.hex", timeoutSecs=4)
                print "min_value:", min_value, "execExpr:", execExpr
                h2o.verboseprint("min: ", min_value, "trial:", trial)

if __name__ == '__main__':
    h2o.unit_main()
