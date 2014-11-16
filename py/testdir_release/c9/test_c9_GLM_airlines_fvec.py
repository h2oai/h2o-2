import unittest
import random, sys, time, re
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i, h2o_glm, h2o_util, h2o_rf, h2o_jobs as h2j
import h2o_common, h2o_gbm

class releaseTest(h2o_common.ReleaseCommon, unittest.TestCase):

    def test_c9_GLM_airlines_fvec(self):
        files = [
                 ('airlines', 'airlines_all.csv', 'airlines_all.hex', 1800, 'IsDepDelayed')
                ]

        for importFolderPath, csvFilename, trainKey, timeoutSecs, response in files:
            # PARSE train****************************************
            csvPathname = importFolderPath + "/" + csvFilename
            
            start = time.time()
            parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, schema='local', hex_key=trainKey, 
                timeoutSecs=timeoutSecs)
            elapsed = time.time() - start
            print "parse end on ", csvFilename, 'took', elapsed, 'seconds',\
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
            print "parse result:", parseResult['destination_key']

            # GLM (train)****************************************
            params = {
                # 'lambda': 1e-4,
                # 'alpha': 0.5,
                'lambda': 1e-8,
                'alpha': 0.0,
                'max_iter': 10,
                'n_folds': 3,
                'family': 'binomial',
                'destination_key': "GLMKEY",
                'response': response,
                'ignored_cols': 'CRSDepTime,CRSArrTime,ActualElapsedTime,CRSElapsedTime,AirTime,ArrDelay,DepDelay,TaxiIn,TaxiOut,Cancelled,CancellationCode,Diverted,CarrierDelay,WeatherDelay,NASDelay,SecurityDelay,LateAircraftDelay,IsArrDelayed'
            }
            kwargs = params.copy()
            timeoutSecs = 1800
            start = time.time()
            glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=timeoutSecs,**kwargs)
            elapsed = time.time() - start
            print "GLM training completed in", elapsed, "seconds. On dataset: ", csvFilename
            h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)

            modelKey = glm['glm_model']['_key']

            submodels = glm['glm_model']['submodels']
            # hackery to make it work when there's just one
            validation = submodels[-1]['validation']
            best_threshold = validation['best_threshold']
            thresholds = validation['thresholds']
            # have to look up the index for the cm, from the thresholds list
            best_index = None
            for i,t in enumerate(thresholds):
                if t == best_threshold:
                    best_index = i
                    break
            cms = validation['_cms']
            cm = cms[best_index]
            pctWrong = h2o_gbm.pp_cm_summary(cm['_arr']);
            # FIX! should look at prediction error/class error?
            # self.assertLess(pctWrong, 9,"Should see less than 40% error")

            print "\nTrain\n==========\n"
            print h2o_gbm.pp_cm(cm['_arr'])

            # Score *******************************
            # this messes up if you use case_mode/case_vale above
            predictKey = 'Predict.hex'
            start = time.time()

            predictResult = h2o_cmd.runPredict(
                data_key=trainKey,
                model_key=modelKey,
                destination_key=predictKey,
                timeoutSecs=timeoutSecs)

            predictCMResult = h2o.nodes[0].predict_confusion_matrix(
                actual=trainKey,
                vactual=response,
                predict=predictKey,
                vpredict='predict',
                )

            cm = predictCMResult['cm']
            # These will move into the h2o_gbm.py
            pctWrong = h2o_gbm.pp_cm_summary(cm);
            # self.assertLess(pctWrong, 40,"Should see less than 40% error")

            print "\nTest\n==========\n"
            print h2o_gbm.pp_cm(cm)


        h2i.delete_keys_at_all_nodes(timeoutSecs=600)


if __name__ == '__main__':
    h2o.unit_main()
