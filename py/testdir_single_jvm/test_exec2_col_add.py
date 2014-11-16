import unittest, random, sys, time, getpass, re
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_browse as h2b, h2o_exec as h2e, h2o_import as h2i, h2o_cmd
import h2o_gbm

DO_PLOT = getpass.getuser()=='kevin'
DO_PLOT = True
DO_ORIG = True

initList = [
        'r2 = c(1); r2 = r1[,c(1)]',
        ]
exprList = [
        'r1[,1] = Last.value = r2',
        'apply(r1,2,sum)',
        ]

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init(java_heap_GB=12, java_extra_args='-XX:+PrintGCDetails')


    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    @unittest.skip("Skip RefCnt Failing Test")
    def test_exec2_col_add(self):
        bucket = 'home-0xdiag-datasets'
        # csvPathname = 'airlines/year2013.csv'
        if h2o.localhost:
            # csvPathname = '1B/reals_100000x1000_15f.data'
            # csvPathname = '1B/reals_1000000x1000_15f.data'
            csvPathname = '1B/reals_1000000x1_15f.data'
            # csvPathname = '1B/reals_1B_15f.data'
            # csvPathname = '1B/reals_100M_15f.data'
        else:
            # csvPathname = '1B/reals_100000x1000_15f.data'
            # csvPathname = '1B/reals_1000000x1000_15f.data'
            csvPathname = '1B/reals_1000000x1_15f.data'
            # csvPathname = '1B/reals_1B_15f.data'
            # csvPathname = '1B/reals_100M_15f.data'

        hex_key = 'r1'
        parseResult = h2i.import_parse(bucket=bucket, path=csvPathname, schema='local', 
            hex_key=hex_key, timeoutSecs=3000, retryDelaySecs=2, doSummary=False)
        inspect = h2o_cmd.runInspect(key=hex_key)
        print "numRows:", inspect['numRows']
        print "numCols:", inspect['numCols']
        inspect = h2o_cmd.runInspect(key=hex_key, offset=-1)
        print "inspect offset = -1:", h2o.dump_json(inspect)

        xList = []
        eList = []
        fList = []
        for execExpr in initList:
            execResult, result = h2e.exec_expr(h2o.nodes[0], execExpr, resultKey=None, timeoutSecs=300)
        for trial in range(1000):
            for execExpr in exprList:
                # put the trial number into the temp for uniqueness
                execExpr = re.sub('Last.value', 'Last.value%s' % trial, execExpr)
                execExpr = re.sub(',1', ',%s' % trial, execExpr)
                start = time.time()
                execResult, result = h2e.exec_expr(h2o.nodes[0], execExpr, resultKey=None, timeoutSecs=300)
                execTime = time.time() - start
                print 'exec took', execTime, 'seconds'
                c = h2o.nodes[0].get_cloud()
                c = c['nodes']

                # print (h2o.dump_json(c))
                k = [i['num_keys'] for i in c]
                v = [i['value_size_bytes'] for i in c]

                
                print "keys: %s" % " ".join(map(str,k))
                print "value_size_bytes: %s" % " ".join(map(str,v))

                # print "result:", result
                if ('r1' in execExpr) and (not 'apply' in execExpr):
                    xList.append(trial)
                    eList.append(execTime)
                if ('apply' in execExpr):
                    fList.append(execTime)

        h2o.check_sandbox_for_errors()
        # PLOTS. look for eplot.jpg and fplot.jpg in local dir?
        if DO_PLOT:
            xLabel = 'trial'
            eLabel = 'time: r1[,1] = Last.value = r2',
            fLabel = 'time: apply(r1, 2, sum)',
            eListTitle = ""
            fListTitle = ""
            h2o_gbm.plotLists(xList, xLabel, eListTitle, eList, eLabel, fListTitle, fList, fLabel, server=True)




if __name__ == '__main__':
    h2o.unit_main()
