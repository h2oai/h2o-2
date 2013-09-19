import unittest
import random, sys, time, re
sys.path.extend(['.','..','py'])

def plotit(xList, eList, sList):
    if h2o.python_username!='kevin':
        return
    
    import pylab as plt
    if eList:
        print "xList", xList
        print "eList", eList
        print "sList", sList

        label = "1jvmx28GB Covtype GBM learn_rate=.2 nbins=1024 ntrees=40 min_rows = 10"
        plt.figure()
        plt.plot (xList, eList)
        plt.xlabel('max_depth')
        plt.ylabel('error')
        plt.title(label)
        plt.draw()

        label = "1jvmx28GB Covtype GBM learn_rate=.2 nbins=1024 ntrees=40 min_rows = 10"
        plt.figure()
        plt.plot (xList, sList)
        plt.xlabel('max_depth')
        plt.ylabel('time')
        plt.title(label)
        plt.draw()

        plt.show()


import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import as h2i, h2o_glm, h2o_util, h2o_rf, h2o_jobs as h2j
class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(1, java_heap_GB=28)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GBM_parseTrain(self):
        h2o.beta_features = False
        bucket = 'home-0xdiag-datasets'

        files = [#('standard', 'covtype200x.data', 'covtype.hex', 1800, 54),
                 #('mnist', 'mnist8m.csv', 'mnist8m.hex',1800,0),
                 #('manyfiles-nflx-gz', 'file_95.dat.gz', 'nflx.hex',1800,256),
                 #('standard', 'allyears2k.csv', 'allyears2k.hex',1800,'IsDepDelayed'),
                 ('standard', 'covtype.data', 'covtype.hex', 1800, 54)
                ]

        for importFolderPath, csvFilename, trainKey, timeoutSecs, vresponse in files:
            h2o.beta_features = False #turn off beta_features
            # PARSE train****************************************
            start = time.time()
            h2o.beta_features = True

            print "Parsing to fvec directly! Have to noPoll=true!, and doSummary=False!"
            parseResult = h2i.import_parse(bucket=bucket, path=importFolderPath + "/" + csvFilename, schema='local',
                hex_key=trainKey, timeoutSecs=timeoutSecs, noPoll=True, doSummary=False)
            # hack
            if h2o.beta_features:
                h2j.pollWaitJobs(pattern="GBMKEY", timeoutSecs=1800, pollTimeoutSecs=1800)
                print "Filling in the parseResult['destination_key'] for h2o"
                parseResult['destination_key'] = trainKey

            elapsed = time.time() - start
            print "parse end on ", csvFilename, 'took', elapsed, 'seconds',\
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
            print "parse result:", parseResult['destination_key']

            xList = []
            eList = []
            sList = []
            # GBM (train)****************************************
            # for depth in [5]:
            # depth = 5
            # for ntrees in [10,20,40,80,160]:
            ntrees = 40
            for max_depth in [5,10,20,40]:
            # for ntrees in [1,2,3,4]:
                params = {
                    'destination_key': "GBMKEY",
                    'learn_rate': .2,
                    'nbins': 1024,
                    'ntrees': ntrees,
                    'max_depth': max_depth,
                    'min_rows': 10,
                    'vresponse': vresponse,
                    # 'ignored_cols': 
                }
                print "Using these parameters for GBM: ", params
                kwargs = params.copy()

                # translate it
                h2o_cmd.runInspect(key=parseResult['destination_key'])
                ### h2o_cmd.runSummary(key=parseResult['destination_key'])
                start = time.time()
                GBMResult = h2o_cmd.runGBM(parseResult=parseResult, noPoll=True,timeoutSecs=timeoutSecs,**kwargs)
                # hack
                if h2o.beta_features:
                    h2j.pollWaitJobs(pattern="GBMKEY", timeoutSecs=1800, pollTimeoutSecs=1800)
                elapsed = time.time() - start
                print "GBM training completed in", elapsed, "seconds. On dataset: ", csvFilename

                GBMView = h2o_cmd.runGBMView(model_key='GBMKEY')
                # errrs from end of list? is that the last tree?
                errsLast = GBMView['gbm_model']['errs'][-1]
                print "GBM 'errsLast'", errsLast

                # xList.append(ntrees)
                xList.append(max_depth)
                eList.append(errsLast)
                sList.append(elapsed)

            plotit(xList, eList, sList)

if __name__ == '__main__':
    h2o.unit_main()
