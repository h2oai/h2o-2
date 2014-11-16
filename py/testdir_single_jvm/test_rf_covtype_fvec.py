import unittest, random, sys, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_rf as h2o_rf, h2o_import as h2i, h2o_exec, h2o_jobs, h2o_gbm

paramDict = {
    'response': 'C55',
    'cols': None,
    # 'ignored_cols_by_name': 'C1,C2,C6,C7,C8',
    'ignored_cols_by_name': None,
    'classification': 1, 
    'validation': None,
    # fail case
    # 'ntrees': 1,
    # 'max_depth': 30,
    # 'nbins': 100,
    'ntrees': 10,
    'max_depth': 20,
    
    'min_rows': 1, # normally 1 for classification, 5 for regression
    'nbins': 200,
    'mtries': None,
    'sample_rate': 0.66,
    'importance': 0,
    'seed': None,
    }

DO_OOBE = False
DO_PLOT = True
# TRY = 'max_depth'
# TRY = 'ntrees'
TRY = 'nbins'

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init(1, java_heap_GB=4)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_rf_covtype_fvec(self):
        importFolderPath = "standard"

        # Parse Train ******************************************************
        csvTrainFilename = 'covtype.shuffled.90pct.data'
        csvTrainPathname = importFolderPath + "/" + csvTrainFilename
        hex_key = csvTrainFilename + ".hex"
        parseTrainResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvTrainPathname, hex_key=hex_key, 
            timeoutSecs=180, doSummary=False)
        inspect = h2o_cmd.runInspect(None, parseTrainResult['destination_key'])

        # Parse Test ******************************************************
        csvTestFilename = 'covtype.shuffled.10pct.data'
        csvTestPathname = importFolderPath + "/" + csvTestFilename
        hex_key = csvTestFilename + ".hex"
        parseTestResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvTestPathname, hex_key=hex_key, 
            timeoutSecs=180)
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
            if DO_OOBE:
                paramDict['validation'] = None
            else:
                paramDict['validation'] = parseTestResult['destination_key']

            timeoutSecs = 30 + paramDict['ntrees'] * 200

            
            # do ten starts, to see the bad id problem?
            TRIES = 5
            for i in range(TRIES):
                lastOne = i==(TRIES-1)

                # have unique model names
                trial += 1
                kwargs = paramDict.copy()
                model_key = 'RFModel_' + str(trial)
                kwargs['destination_key'] = model_key
                data_key = parseTrainResult['destination_key']

                start = time.time()
                rfResult = h2o_cmd.runRF(parseResult=parseTrainResult, timeoutSecs=timeoutSecs, 
                    noPoll=True, rfView=False, **kwargs)
                trainElapsed = time.time() - start
                print 'rf train end', i, 'on', csvTrainPathname, 'took', trainElapsed, 'seconds'

                # don't cancel the last one
                if not lastOne:
                    time.sleep(1) 
                    h2o_jobs.cancelAllJobs(timeoutSecs=2)


            ### print "rfView", h2o.dump_json(rfView)
            print "We have a result from the RF above, completed but didn't do RFView yet"
            # could the RF indicate 'done' too soon?
            # if rfResult['state']=='RUNNING':
            #    raise Exception("Why is this RF still in RUNNING state? %s" % h2o.dump_json(rfResult))

            # if 'drf_model' not in rfResult:
            #    raise Exception("How come there's no drf_model in this RF result? %s" % h2o.dump_json(rfResult))
            h2o_jobs.pollWaitJobs(timeoutSecs=300)
            rfView = h2o_cmd.runRFView(None, model_key=model_key, timeoutSecs=60, retryDelaySecs=5, doSimpleCheck=False)
            print "rfView:", h2o.dump_json(rfView)
            

            rf_model = rfView['drf_model']
            cms = rf_model['cms']
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

            (classification_error, classErrorPctList, totalScores) = h2o_rf.simpleCheckRFView(rfv=rfView)
            # we iterate over params, so can't really do this check
            # self.assertAlmostEqual(classification_error, 0.03, delta=0.5, msg="Classification error %s differs too much" % classification_error)

            print "classErrorPctList:", classErrorPctList
            self.assertEqual(len(classErrorPctList), 7, "Should be 7 output classes, so should have 7 class error percentages from a reasonable predict")
            # FIX! should update this expected classification error
            predict = h2o.nodes[0].generate_predictions(model_key=model_key, data_key=data_key)

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
