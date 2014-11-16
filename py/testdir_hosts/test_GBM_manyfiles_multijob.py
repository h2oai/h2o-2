import unittest
import random, sys, time, re
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i, h2o_glm, h2o_util, h2o_rf, h2o_jobs as h2j, h2o_gbm

DO_FAIL = False
DO_CLASSIFICATION = True

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init(1, java_heap_GB=12)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GBM_manyfiles_multijob(self):
        bucket = 'home-0xdiag-datasets'
        modelKey = 'GBMModelKey'
        if h2o.localhost:
            files = [
                # None forces numCols to be used. assumes you set it from Inspect
                # problems with categoricals not in the train data set? (warnings in h2o stdout)
                ## ('manyfiles-nflx-gz', 'file_1.dat.gz', 'file_1.hex', 1800, None, 'file_11.dat.gz', 'test.hex')
                # just use matching
                ('manyfiles-nflx-gz', 'file_1.dat.gz', 'train.hex', 1800, None, 'file_1.dat.gz', 'test.hex')
                ]
        else:
            files = [
                # None forces numCols to be used. assumes you set it from Inspect
                ('manyfiles-nflx-gz', 'file_[0-9].dat.gz', 'train.hex', 1800, None, 'file_1[0-9].dat.gz', 'test.hex')
                ]

        # if I got to hdfs, it's here
        # hdfs://172.16.2.176/datasets/manyfiles-nflx-gz/file_99.dat.gz

        # h2b.browseTheCloud()
        for (importFolderPath, trainFilename, trainKey, timeoutSecs, response, testFilename, testKey) in files:
            # PARSE train****************************************
            start = time.time()
            xList = []
            eList = []
            fList = []

            # Parse (train)****************************************
            csvPathname = importFolderPath + "/" + trainFilename
            parseTrainResult = h2i.import_parse(bucket=bucket, path=csvPathname, schema='local',
                hex_key=trainKey, timeoutSecs=timeoutSecs, doSummary=False)
            elapsed = time.time() - start
            print "train parse end on ", trainFilename, 'took', elapsed, 'seconds',\
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
            print "train parse result:", parseTrainResult['destination_key']

            ### h2o_cmd.runSummary(key=parsTraineResult['destination_key'])

            inspect = h2o_cmd.runInspect(key=parseTrainResult['destination_key'])
            print "\n" + csvPathname, \
                "    numRows:", "{:,}".format(inspect['numRows']), \
                "    numCols:", "{:,}".format(inspect['numCols'])
            numRows = inspect['numRows']
            numCols = inspect['numCols']

            # Make col 378 it something we can do binomial regression on!
            # execExpr = '%s=colSwap(%s,378,(%s[378]>15 ? 1 : 0))' % (trainKey, trainKey, trainKey)
            # inc by 1 for R col
            # BUG: if left as integer..GBM changes to Enum. multiple jobs collide on this translate
            # only a problem if they share the dataset, do classification with integers.
            # change to factor here, to avoid the problem
            execExpr = '%s[,378+1]=%s[,378+1]>15' % (trainKey, trainKey)
            if not DO_FAIL:
                execExpr +=  "; factor(%s[, 378+1]);" % (trainKey)

            resultExec = h2o_cmd.runExec(str=execExpr, timeoutSecs=180)

            # Parse (test)****************************************
            csvPathname = importFolderPath + "/" + testFilename
            parseTestResult = h2i.import_parse(bucket=bucket, path=csvPathname, schema='local',
                hex_key=testKey, timeoutSecs=timeoutSecs, doSummary=False)
            elapsed = time.time() - start
            print "test parse end on ", testFilename, 'took', elapsed, 'seconds',\
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
            print "test parse result:", parseTestResult['destination_key']

            # Make col 378 it something we can do binomial regression on!
            # plus 1 for R indexing
            execExpr = '%s[,378+1]=%s[,378+1]>15' % (testKey, testKey)
            if not DO_FAIL:
                execExpr +=  "; factor(%s[, 378+1]);" % (testKey)
            resultExec = h2o_cmd.runExec(str=execExpr, timeoutSecs=180)

            # Note ..no inspect of test data here..so translate happens later?

            # GBM (train iterate)****************************************
            # if not response:
            #     response = numCols - 1
            response = 378

            # randomly ignore a bunch of cols, just to make it go faster
            x = range(numCols)
            del x[response]
            # add 1 for start-with-1
            ignored_cols_by_name = ",".join(map(lambda x: "C" + str(x+1), random.sample(x, 300)))

            print "Using the same response %s for train and test (which should have a output value too)" % 'C' + str(response+1)

            ntrees = 10
            trial = 0
            # ignore 200 random cols (not the response)
            print "Kicking off multiple GBM jobs at once"
            # GBM train****************************************
            if DO_FAIL:
                cases = [5, 10, 20, 40]
            else:
                cases = [5, 10, 20]

            for max_depth in cases:
                trial += 1

                params = {
                    'response': "C" + str(response+1),
                    'learn_rate': .2,
                    'nbins': 1024,
                    'ntrees': ntrees,
                    'max_depth': max_depth,
                    'min_rows': 10,
                    'validation': parseTestResult['destination_key'],
                    'ignored_cols_by_name': ignored_cols_by_name,
                    'grid_parallelism': 1,
                    'classification': 1 if DO_CLASSIFICATION else 0,
                }
            

                ### print "Using these parameters for GBM: ", params
                kwargs = params.copy()

                trainStart = time.time()
                # can take 4 times as long with 4 jobs?
                gbmTrainResult = h2o_cmd.runGBM(parseResult=parseTrainResult,
                    noPoll=True, timeoutSecs=timeoutSecs * 4, destination_key=modelKey + "_" + str(trial), **kwargs)
                trainElapsed = time.time() - trainStart
                print "GBM dispatch completed in", trainElapsed, "seconds. On dataset: ", trainFilename


            statMean = h2j.pollStatsWhileBusy(timeoutSecs=timeoutSecs, pollTimeoutSecs=timeoutSecs, retryDelaySecs=5)
            num_cpus = statMean['num_cpus'],
            my_cpu_pct = statMean['my_cpu_%'],
            sys_cpu_pct = statMean['sys_cpu_%'],
            system_load = statMean['system_load']

            h2j.pollWaitJobs(timeoutSecs=timeoutSecs, pollTimeoutSecs=timeoutSecs)


if __name__ == '__main__':
    h2o.unit_main()
