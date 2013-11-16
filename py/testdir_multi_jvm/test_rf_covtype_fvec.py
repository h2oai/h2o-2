import unittest, random, sys, time
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_rf as h2o_rf, h2o_hosts, h2o_import as h2i, h2o_exec, h2o_jobs, h2o_gbm

# we can pass ntree thru kwargs if we don't use the "trees" parameter in runRF
# only classes 1-7 in the 55th col
# rng DETERMINISTIC is default

paramDict = {
    'response': 'C54',
    'cols': None,
    # 'ignored_cols_by_name': 'C1,C2,C6,C7,C8',
    'ignored_cols_by_name': None,
    'classification': 1, # regression
    'validation': None,
    'ntrees': 1,
    'max_depth': 30,
    'min_rows': 2, # normally 1 for classification, 5 for regression
    'nbins': 100,
    'mtries': None,
    'sample_rate': 0.66,
    'importance': 0,
    'seed': None,

    }


DO_OOBE = False
DO_PLOT = True
TRY = 'ntrees'    

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(node_count=3, java_heap_GB=4)
        else:
            h2o_hosts.build_cloud_with_hosts(node_count=1, java_heap_GB=10)
        h2o.beta_features = True # fvec

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_rf_covtype_fvec(self):
        importFolderPath = "standard"

        # Parse Train ******************************************************
        csvTrainFilename = 'covtype.shuffled.90pct.data'
        csvTrainPathname = importFolderPath + "/" + csvTrainFilename
        hex_key = csvTrainFilename + ".hex"
        parseTrainResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvTrainPathname, hex_key=hex_key, timeoutSecs=180)
        inspect = h2o_cmd.runInspect(None, parseTrainResult['destination_key'])

        # Parse Test ******************************************************
        csvTestFilename = 'covtype.shuffled.10pct.data'
        csvTestPathname = importFolderPath + "/" + csvTestFilename
        hex_key = csvTestFilename + ".hex"
        parseTestResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvTestPathname, hex_key=hex_key, timeoutSecs=180)
        inspect = h2o_cmd.runInspect(None, parseTestResult['destination_key'])

        rfViewInitial = []
        xList = []
        eList = []
        fList = []
        trial = 0

        depthList  = [10, 20, 30, 40]
        ntreesList = [5, 10, 20, 30]
        # ntreesList = [2]
        nbinsList  = [10, 100, 1000]

        if TRY == 'max_depth':
            tryList = depthList
        elif TRY == 'ntrees':
            tryList = ntreesList
        elif TRY == 'nbins':
            tryList = nbinsList
        else:
            raise Exception("huh? %s" % TRY)

        for d in tryList:
            if TRY == 'max_depth':
                paramDict['max_depth'] = d
            elif TRY == 'ntrees':
                paramDict['ntrees'] = d
            elif TRY == 'nbins':
                paramDict['nbins'] = d
            else:
                raise Exception("huh? %s" % TRY)

            # adjust timeoutSecs with the number of trees
            # seems ec2 can be really slow
            trial += 1
            paramDict['destination_key'] = 'RFModel_' + str(trial)
            if DO_OOBE:
                paramDict['validation'] = None
            else:
                paramDict['validation'] = parseTestResult['destination_key']

            kwargs = paramDict.copy()
            timeoutSecs = 30 + kwargs['ntrees'] * 200

            start = time.time()
            rfFirstResult = h2o_cmd.runRF(parseResult=parseTrainResult, timeoutSecs=timeoutSecs, noPoll=False, rfView=False, **kwargs)
            # print h2o.dump_json(rfFirstResult)
            # FIX! are these already in there?
            rfView = {}
            rfView['data_key'] = hex_key
            rfView['model_key'] = kwargs['destination_key']
            rfView['ntrees'] = kwargs['ntrees']

            h2o_jobs.pollWaitJobs(timeoutSecs=300, pollTimeoutSecs=120, retryDelaySecs=5)
            trainElapsed = time.time() - start
            print "rf train end on ", csvTrainPathname, 'took', trainElapsed, 'seconds'
            ### print "rfView", h2o.dump_json(rfView)
            data_key = rfView['data_key']
            model_key = rfView['model_key']
            ntrees = rfView['ntrees']

            rfView = h2o_cmd.runRFView(None, model_key=model_key, timeoutSecs=60, noPoll=False, doSimpleCheck=False)
            h2o_jobs.pollWaitJobs(timeoutSecs=300, pollTimeoutSecs=300, retryDelaySecs=5)
            # rfView = h2o_cmd.runRFView(None, data_key, model_key, timeoutSecs=60, noPoll=True, doSimpleCheck=False)
            ## print "rfView:", h2o.dump_json(rfView)

            # "N":1,
            # "errs":[0.25,0.1682814508676529],
            # "testKey":"syn_binary_10000x10.hex",
            # "cm":[[3621,1399],[1515,3465]]}}

            rf_model = rfView['drf_model']
            cm = rf_model['cm']
            ### print "cm:", h2o.dump_json(cm)
            ntrees = rf_model['N']
            errs = rf_model['errs']
            N = rf_model['N']
            varimp = rf_model['varimp']
            treeStats = rf_model['treeStats']

            print "maxDepth:", treeStats['maxDepth']
            print "maxLeaves:", treeStats['maxLeaves']
            print "minDepth:", treeStats['minDepth']
            print "minLeaves:", treeStats['minLeaves']
            print "meanLeaves:", treeStats['meanLeaves']
            print "meanDepth:", treeStats['meanDepth']
            print "errs[0]:", errs[0]
            print "errs[-1]:", errs[-1]
            print "errs:", errs

            (error, classErrorPctList, totalScores) = h2o_rf.simpleCheckRF2View(rfv=rfView)

            # "treeStats": {
            #     "maxDepth": 15, 
            #     "maxLeaves": 1715, 
            #     "meanDepth": 15, 
            #     "meanLeaves": 421, 
            #     "minDepth": 15, 
            #     "minLeaves": 5
            # }, 
            # "varimp": null

            # FIX! should update this expected classification error
            ## (classification_error, classErrorPctList, totalScores) = h2o_rf.simpleCheckRFView(rfv=rfView, ntree=ntrees)
            ## self.assertAlmostEqual(classification_error, 0.03, delta=0.5, msg="Classification error %s differs too much" % classification_error)
            predict = h2o.nodes[0].generate_predictions(model_key=model_key, data_key=data_key)

            # xList.append(ntrees)
            # paramDict['max_depth']
            # paramDict['nbins']
            # paramDict['mtries']
            # paramDict['ntrees']
            # eList.append(errs[-1])
            eList.append(classErrorPctList[4])
            fList.append(trainElapsed)
            if DO_PLOT:
                if TRY == 'max_depth':
                    xLabel = 'max_depth'
                elif TRY == 'ntrees':
                    xLabel = 'ntrees'
                elif TRY == 'nbins':
                    xLabel = 'nbins'
                else:
                    raise Exception("huh? %s" % TRY)
                xList.append(paramDict[xLabel])

        if DO_PLOT:
            eLabel = 'class 4 pctWrong'
            fLabel = 'trainElapsed'
            eListTitle = ""
            fListTitle = ""
            h2o_gbm.plotLists(xList, xLabel, eListTitle, eList, eLabel, fListTitle, fList, fLabel)

if __name__ == '__main__':
    h2o.unit_main()
