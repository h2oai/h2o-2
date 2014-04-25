import unittest, random, sys, time
sys.path.extend(['.','..','py'])
import h2o, h2o_browse as h2b, h2o_exec as h2e, h2o_hosts, h2o_import as h2i, h2o_cmd

# new ...ability to reference cols
# src[ src$age<17 && src$zip=95120 && ... , ]
# can specify values for enums ..values are 0 thru n-1 for n enums
print "FIX!: need to test the && and || reduction operators"
initList = [
        ]

exprList = [
        'a=c(1); a = sum(r1[1,])',
        'b=c(1); b = sum(r1[1,])',
        'd=c(1); d = sum(r1[1,])',
        'e=c(1); e = sum(r1[1,])',
        'f=c(1); f = sum(r1[1,])',
        'f=c(1); g = sum(r1[1,])',
        'h=c(1); h = sum(r1[1,])',
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
            h2o.build_cloud(1, java_heap_GB=28)
        else:
            h2o_hosts.build_cloud_with_hosts(1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_exec2_sum(self):
        h2o.beta_features = True
        bucket = 'home-0xdiag-datasets'
        # csvPathname = 'airlines/year2013.csv'
        csvPathname = '1B/reals_1B_15f.data'
        csvPathname = 'standard/covtype.data'
        csvPathname = '1B/reals_1000_15f.data'

        hex_key = 'r1'
        parseResult = h2i.import_parse(bucket=bucket, path=csvPathname, schema='local', 
            hex_key=hex_key, timeoutSecs=3000, retryDelaySecs=2)
        inspect = h2o_cmd.runInspect(key=hex_key)
        print "numRows:", inspect['numRows']
        print "numCols:", inspect['numCols']
        inspect = h2o_cmd.runInspect(key=hex_key, offset=-1)
        print "inspect offset = -1:", h2o.dump_json(inspect)

        for execExpr in exprList:
            start = time.time()
            execResult, result = h2e.exec_expr(h2o.nodes[0], execExpr, resultKey=None, timeoutSecs=300)
            print 'exec took', time.time() - start, 'seconds'
            print "result:", result

        h2o.check_sandbox_for_errors()


if __name__ == '__main__':
    h2o.unit_main()
