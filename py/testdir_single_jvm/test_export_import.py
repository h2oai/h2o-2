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

    def test_export_import(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        importFolderPath = "standard"

        # Parse Train ******************************************************
        csvTrainFilename = 'covtype.shuffled.90pct.data'
        csvTrainPathname = importFolderPath + "/" + csvTrainFilename
        trainKey = csvTrainFilename + ".hex"
        parseTrainResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvTrainPathname, hex_key=trainKey,
            timeoutSecs=180, doSummary=False)
        inspect = h2o_cmd.runInspect(None, trainKey)

        # Parse Test ******************************************************
        csvTestFilename = 'covtype.shuffled.10pct.data'
        csvTestPathname = importFolderPath + "/" + csvTestFilename
        testKey = csvTestFilename + ".hex"
        parseTestResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvTestPathname, hex_key=testKey,
            timeoutSecs=180)
        inspect = h2o_cmd.runInspect(None, testKey)


        trial = 0
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
            trial += 1
            kwargs = paramDict.copy()
            modelKey = 'RFModel_' + str(trial)
            kwargs['destination_key'] = modelKey

            start = time.time()
            rfResult = h2o_cmd.runRF(parseResult=parseTrainResult, timeoutSecs=timeoutSecs, **kwargs)
            trainElapsed = time.time() - start
            print 'rf train end on', csvTrainPathname, 'took', trainElapsed, 'seconds'

            h2o.nodes[0].export_files(src_key=testKey, path=SYNDATASETS_DIR + "/" + testKey, force=1)
            h2o.nodes[0].export_files(src_key=trainKey, path=SYNDATASETS_DIR + "/" + trainKey, force=1)
            # h2o.nodes[0].export_files(src_key=modelKey, path=SYNDATASETS_DIR + "/" + modelKey, force=1)


            rf_model = rfResult['drf_model']
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

            (classification_error, classErrorPctList, totalScores) = h2o_rf.simpleCheckRFView(rfv=rfResult)
            print "classErrorPctList:", classErrorPctList
            self.assertEqual(len(classErrorPctList), 7, "Should be 7 output classes, so should have 7 class error percentages from a reasonable predict")
            # FIX! should update this expected classification error
            predict = h2o.nodes[0].generate_predictions(model_key=modelKey, data_key=testKey)

if __name__ == '__main__':
    h2o.unit_main()
