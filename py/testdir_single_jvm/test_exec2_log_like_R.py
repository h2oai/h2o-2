import unittest, random, sys, time, getpass, re
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_browse as h2b, h2o_exec as h2e, h2o_import as h2i, h2o_cmd
import h2o_gbm

DO_PLOT = getpass.getuser()=='kevin'
DO_PLOT = True
DO_ORIG = True

# new ...ability to reference cols
# src[ src$age<17 && src$zip=95120 && ... , ]
# can specify values for enums ..values are 0 thru n-1 for n enums
# the inspect info looks like this for the 8MB temps created..just 4 chunks
# Row C1
# Change Type 
# Type    Real
# Min 4.221149880967445E-6
# Max 1.999996683238248
# Mean    0.999
# Size    7.6 MB
# /172.16.2.41:54321, 0  C8D
# /172.16.2.41:54321, 228060 C8D
# /172.16.2.41:54321, 456150 C8D
# /172.16.2.41:54321, 684255 C8D

# Do Execs not do compression?

initList = [
        'r2 = c(1); r2 = r1[,c(1)]',
        ]
if DO_ORIG:
    # update to write back to the original dataset at the same time as the last temp
    exprList = [
        'Last.value.0 = r1[,c(1)]',
        'Last.value.1 = any.factor(Last.value.0)',
        'Last.value.2 = Last.value.0 + 1',
        'r1[,1] = Last.value.3 = log(Last.value.2)',

        'Last.value.4 = r2',
        'Last.value.5 = any.factor(Last.value.4)',
        'Last.value.6 = Last.value.4 + 1',
        'r1[,1] = Last.value.7 = log(Last.value.6)',

        'Last.value.8 = r2',
        'Last.value.9 = any.factor(Last.value.8)',
        'Last.value.10 = Last.value.8 + 1',
        'r1[,1] = Last.value.11 = log(Last.value.10)',

        ]
else:
    exprList = [
        'Last.value.3 = r2+1',
        ]

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init(java_heap_GB=14, java_extra_args='-XX:+PrintGCDetails')


    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_exec2_log_like_R(self):
        bucket = 'home-0xdiag-datasets'
        csvPathname = 'airlines/year2013.csv'
        # csvPathname = '1B/reals_100000x1000_15f.data'
        # csvPathname = '1B/reals_1000000x1000_15f.data'
        # csvPathname = '1B/reals_1000000x1_15f.data'
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
        for trial in range(300):
            for execExpr in exprList:
                # put the trial number into the temp for uniqueness
                execExpr = re.sub('Last.value', 'Last.value%s' % trial, execExpr)
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
                if DO_ORIG:
                    if 'r1' in execExpr:
                        xList.append(trial)
                        eList.append(execTime)
                    if 'log' in execExpr:
                        fList.append(execTime)
                else:
                    xList.append(trial)
                    eList.append(execTime)
                    fList.append(execTime)

        h2o.check_sandbox_for_errors()
        # PLOTS. look for eplot.jpg and fplot.jpg in local dir?
        if DO_PLOT:
            xLabel = 'trial'
            if DO_ORIG:
                eLabel = 'time: Last.value<trial>.4 = r1[,c(1)]'
                fLabel = 'time: Last.value<trial>.7 = log(Last.value<trial>.6)'
            else:
                eLabel = 'time: Last.value.3 = r2+1'
                fLabel = 'time: Last.value.3 = r2+1'
            eListTitle = ""
            fListTitle = ""
            h2o_gbm.plotLists(xList, xLabel, eListTitle, eList, eLabel, fListTitle, fList, fLabel, server=True)




if __name__ == '__main__':
    h2o.unit_main()
