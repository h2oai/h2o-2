import unittest, random, sys, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i, h2o_gbm, h2o_jobs as h2j, h2o_import
import h2o_exec as h2e, h2o_util
import math


print "Copy a version of this to a two cloud test. different failure mode"
DO_PLOT = True
COL = 1
PHRASE = "func1"
FUNC_PHRASE = "func1=function(x){max(x[,%s])}" % COL

DO_KNOWN_FAIL = False
DO_APPEND_KNOWN_FAIL2 = False
DO_REALS = False

CLOUD_SIZE = 2

initList = [
    (None, FUNC_PHRASE),
    # (None, "func2=function(x){a=3;nrow(x[,%s])*a}" % COL),
    # (None, "func3=function(x){apply(x[,%s],2,sum)/nrow(x[,%s])}" % (COL, col) ),
    # (None, "function(x) { cbind( mean(x[,1]), mean(x[,%s]) ) }" % COL),
    # (None, "func4=function(x) { mean( x[,%s]) }" % COL),
    # (None, "func5=function(x) { sd( x[,%s]) }" % COL),
    # (None, "func6=function(x) { quantile(x[,%s] , c(0.9) ) }" % COL),
]

print "Data is all integers, minInt to maxInt..so it shouldn't have fp roundoff errors while summing the row counts I use?"
def write_syn_dataset(csvPathname, rowCount, colCount, minInt, maxInt, SEED):
    r1 = random.Random(SEED)
    dsf = open(csvPathname, "w+")

    for i in range(rowCount):
        rowData = []

        if DO_REALS:
            for j in range(colCount):
                # maybe do a significatly smaller range than min/max ints.
                # divide by pi to get some non-integerness
                ri = r1.randint(minInt,maxInt) / math.pi
                # make it a real?
                rowData.append("%+e" % ri)
        else:
            for j in range(colCount):
                # maybe do a significatly smaller range than min/max ints.
                ri = r1.randint(minInt,maxInt)
                rowData.append(ri)


        rowDataCsv = ",".join(map(str,rowData))
        dsf.write(rowDataCsv + "\n")

    dsf.close()

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init(CLOUD_SIZE,java_heap_GB=12/CLOUD_SIZE)

    @classmethod
    def tearDownClass(cls):
        ### time.sleep(3600)
        h2o.tear_down_cloud()

    def test_ddply_plot(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()

        if DO_KNOWN_FAIL:
            tryList = [
                (1000000, 5, 'cD', 0, 320, 30), 
            ]
        else:
            tryList = [
                (1000000, 5, 'cD', 0, 10, 30), 
                (1000000, 5, 'cD', 0, 20, 30), 
                (1000000, 5, 'cD', 0, 40, 30), 
                (1000000, 5, 'cD', 0, 50, 30), 
                (1000000, 5, 'cD', 0, 80, 30), 
                # (1000000, 5, 'cD', 0, 160, 30), 
                # fails..don't do
                # (1000000, 5, 'cD', 0, 320, 30), 
                # (1000000, 5, 'cD', 0, 320, 30), 
                # starts to fail here. too many groups?
                # (1000000, 5, 'cD', 0, 640, 30), 
                # (1000000, 5, 'cD', 0, 1280, 30), 
                ]

        if DO_APPEND_KNOWN_FAIL2:
            tryList.append(
                (1000000, 5, 'cD', 0, 160, 30), 
            )
            #tryList.append(
            #    (1000000, 5, 'cD', 0, 320, 30), 
            #)
        ### h2b.browseTheCloud()
        xList = []
        eList = []
        fList = []
        trial = 0
        for (rowCount, colCount, hex_key, minInt, maxInt, timeoutSecs) in tryList:
            SEEDPERFILE = random.randint(0, sys.maxint)
            # csvFilename = 'syn_' + str(SEEDPERFILE) + "_" + str(rowCount) + 'x' + str(colCount) + '.csv'
            if DO_KNOWN_FAIL:
                # csvFilename = 'syn_binary_1000000x5.csv.gz' # fails
                # csvFilename = 'a1' # fails
                csvFilename = "syn_ddply_1Mx5_0_320.gz"
                bucket = "home-0xdiag-datasets"
                csvPathname = "standard/" + csvFilename
                minInt = 0
                maxInt = 320
            else:
                bucket = None
                csvFilename = 'syn_' + "binary" + "_" + str(rowCount) + 'x' + str(colCount) + '.csv'
                csvPathname = SYNDATASETS_DIR + '/' + csvFilename
                print "Creating random", csvPathname, "with range", (maxInt-minInt)+1
                write_syn_dataset(csvPathname, rowCount, colCount, minInt, maxInt, SEEDPERFILE)

            for lll in range(1):
                # PARSE train****************************************
                hexKey = 'r.hex'
                parseResult = h2i.import_parse(bucket=bucket, path=csvPathname, schema='put', hex_key=hexKey)
                inspect = h2o_cmd.runInspect(key=hexKey)
                missingValuesList = h2o_cmd.infoFromInspect(inspect, csvFilename)
                self.assertEqual(missingValuesList, [], "a1 should have no NAs in parsed dataset: %s" % missingValuesList)

                for resultKey, execExpr in initList:
                    h2e.exec_expr(h2o.nodes[0], execExpr, resultKey=resultKey, timeoutSecs=60)

                #*****************************************************************************************
                # two columns. so worse case every combination of each possible value
                # only true if enough rows (more than the range?)
                maxExpectedGroups = ((maxInt - minInt) + 1) ** 2
                # do it twice..to get the optimal cached delay for time?
                execExpr = "a1 = ddply(r.hex, c(1,2), " + PHRASE + ")"
                start = time.time()
                (execResult, result) = h2e.exec_expr(h2o.nodes[0], execExpr, resultKey=None, timeoutSecs=500)
                groups = execResult['num_rows']
                # this is a coarse comparision, statistically not valid for small rows, and certain ranges?
                h2o_util.assertApproxEqual(groups, maxExpectedGroups,  rel=0.2, 
                    msg="groups %s isn't close to expected amount %s, minInt: %s maxInt: %s" % (groups, maxExpectedGroups, minInt, maxInt))
                ddplyElapsed = time.time() - start
                print "ddplyElapsed:", ddplyElapsed
                print "execResult", h2o.dump_json(execResult)

                a1dump = h2o_cmd.runInspect(key="a1")
                print "a1", h2o.dump_json(a1dump)
                # should never have any NAs in this result
                missingValuesList = h2o_cmd.infoFromInspect(a1dump, "a1")
                self.assertEqual(missingValuesList, [], "a1 should have no NAs: %s trial: %s" % (missingValuesList, trial))

                #*****************************************************************************************

                execExpr = "a2 = ddply(r.hex, c(1,2), " + PHRASE + ")"
                start = time.time()
                (execResult, result) = h2e.exec_expr(h2o.nodes[0], execExpr, resultKey=None, timeoutSecs=500)
                groups = execResult['num_rows']
                # this is a coarse comparision, statistically not valid for small rows, and certain ranges?
                h2o_util.assertApproxEqual(groups, maxExpectedGroups,  rel=0.2, 
                    msg="groups %s isn't close to expected amount %s, minInt: %s maxInt: %s" % (groups, maxExpectedGroups, minInt, maxInt))
                ddplyElapsed = time.time() - start
                print "ddplyElapsed:", ddplyElapsed
                print "execResult", h2o.dump_json(execResult)

                a2dump = h2o_cmd.runInspect(key="a2")
                print "a2", h2o.dump_json(a2dump)
                # should never have any NAs in this result
                missingValuesList = h2o_cmd.infoFromInspect(a2dump, "a2")
                self.assertEqual(missingValuesList, [], "a2 should have no NAs: %s trial: %s" % (missingValuesList, trial))

                #*****************************************************************************************
                # should be same answer in both cases
                execExpr = "sum(a1!=a2)==0"
                (execResult, result) = h2e.exec_expr(h2o.nodes[0], execExpr, resultKey=None, timeoutSecs=500)
                execExpr = "s=c(0); s=(a1!=a2)"
                (execResult1, result1) = h2e.exec_expr(h2o.nodes[0], execExpr, resultKey=None, timeoutSecs=500)
                print "execResult", h2o.dump_json(execResult)

                #*****************************************************************************************

                # should never have any NAs in this result
                sdump = h2o_cmd.runInspect(key="s")
                print "s", h2o.dump_json(sdump)
                self.assertEqual(result, 1, "a1 and a2 weren't equal? Maybe ddply can vary execution order (fp error? so multiple ddply() can have different answer. %s %s %s" % (FUNC_PHRASE, result, h2o.dump_json(execResult)))

                # xList.append(ntrees)
                trial += 1
                # this is the biggest it might be ..depends on the random combinations
                # groups = ((maxInt - minInt) + 1) ** 2
                xList.append(groups)
                eList.append(ddplyElapsed)
                fList.append(ddplyElapsed)

        if DO_PLOT:
            xLabel = 'groups'
            eLabel = 'ddplyElapsed'
            fLabel = 'ddplyElapsed'
            eListTitle = ""
            fListTitle = ""
            h2o_gbm.plotLists(xList, xLabel, eListTitle, eList, eLabel, fListTitle, fList, fLabel)

if __name__ == '__main__':
    h2o.unit_main()
