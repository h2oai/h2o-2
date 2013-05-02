import unittest
import random, sys, time
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts, h2o_import as h2i, h2o_exec, h2o_glm

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(node_count=1, java_heap_GB=10)
        else:
            h2o_hosts.build_cloud_with_hosts(node_count=1, java_heap_GB=10)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GLM_covtype_train(self):
        print "\nMichal will hate me for another file needed: covtype.shuffled.data"
        importFolderPath = "/home/0xdiag/datasets"
        csvFilename = 'covtype.shuffled.data'
        csvPathname = importFolderPath + "/" + csvFilename
        key2 = csvFilename + ".hex"
        h2i.setupImportFolder(None, importFolderPath)

        print "\nUsing header=0 on the normal covtype.data"
        parseKey = h2i.parseImportFolderFile(None, csvFilename, importFolderPath, key2=key2,
            header=0, timeoutSecs=180)

        inspect = h2o_cmd.runInspect(None, parseKey['destination_key'])
        print "\n" + csvPathname, \
            "    num_rows:", "{:,}".format(inspect['num_rows']), \
            "    num_cols:", "{:,}".format(inspect['num_cols'])

        # how many rows for each pct?
        num_rows = inspect['num_rows']
        pct10 = int(num_rows * .1)
        rowsForPct = [i * pct10 for i in range(0,11)]
        # this can be slightly less than 10%
        last10 = num_rows - rowsForPct[9]
        rowsForPct[10] = last10
        # use mod below for picking "rows-to-do" in case we do more than 9 trials
        # use 10 if 0 just to see (we copied 10 to 0 above)
        rowsForPct[0] = rowsForPct[10]

        print "Creating the key of the last 10% data, for scoring"
        dataKeyTest = "rTest"
        # start at 90% rows + 1
        
        execExpr = dataKeyTest + " = slice(" + key2 + "," + str(rowsForPct[9]+1) + ")"
        h2o_exec.exec_expr(None, execExpr, resultKey=dataKeyTest, timeoutSecs=10)

        kwargs = {
            'y': 54, 
            'max_iter': 20, 
            'n_folds': 0, 
            'thresholds': 0.5,
            'alpha': 0.1, 
            'lambda': 1e-5, 
            'family': 'binomial',
            'case_mode': '=', 
            'case': 2
        }
        timeoutSecs = 60

        for trial in range(10):
            # always slice from the beginning
            rowsToUse = rowsForPct[trial%10] 
            resultKey = "r" + str(trial)
            execExpr = resultKey + " = slice(" + key2 + ",1," + str(rowsToUse) + ")"
            h2o_exec.exec_expr(None, execExpr, resultKey=resultKey, timeoutSecs=10)
            parseKey['destination_key'] = resultKey
            # adjust timeoutSecs with the number of trees
            # seems ec2 can be really slow

            start = time.time()
            glm = h2o_cmd.runGLMOnly(parseKey=parseKey, timeoutSecs=timeoutSecs, pollTimeoutSecs=180, **kwargs)
            print "glm end on ", parseKey['destination_key'], 'took', time.time() - start, 'seconds'
            h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)

            GLMModel = glm['GLMModel']
            modelKey = GLMModel['model_key']

            start = time.time()
            glmScore = h2o_cmd.runGLMScore(key=dataKeyTest, model_key=modelKey, thresholds="0.5", timeoutSecs=timeoutSecs)
            print "glmScore end on ", dataKeyTest, 'took', time.time() - start, 'seconds'
            ### print h2o.dump_json(glmScore)
            classErr = glmScore['validation']['classErr']
            auc = glmScore['validation']['auc']
            err = glmScore['validation']['err']
            print "classErr:", classErr
            print "err:", err
            print "auc:", auc


            print "Trial #", trial, "completed", "using %6.2f" % (rowsToUse*100.0/num_rows), "pct. of all rows"

if __name__ == '__main__':
    h2o.unit_main()
