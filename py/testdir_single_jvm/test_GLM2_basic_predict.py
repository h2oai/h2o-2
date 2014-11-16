import unittest, time, sys
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_glm, h2o_import as h2i

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init(1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_B_GLM2_basic_predict_benign(self):
        h2o.nodes[0].log_view()
        namelist = h2o.nodes[0].log_download()

        print "\nStarting benign.csv"
        csvFilename = "benign.csv"
        csvPathname = 'logreg/' + csvFilename
        hexKey = csvFilename + ".hex"
        parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, hex_key=hexKey, schema='put')
        # columns start at 0
        y = 3
        # cols 0-13. 3 is output
        # no member id in this one
        for maxx in range(1):
            # 3 is output
            kwargs = { 
                'response': y,
                'family': 'binomial',
                'alpha': 0,
                'lambda': 1e-4,
                'n_folds': 0,
            }
            # fails with n_folds
            glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=15, **kwargs)
            h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)
            GLMModel = glm['glm_model']
            modelKey = GLMModel['_key']
            print "Doing predict with same dataset, and the GLM model"
            h2o.nodes[0].generate_predictions(model_key=modelKey, data_key=hexKey, prediction='Predict.hex')

            # just get a predict and AUC on the same data. has to be binomial result
            resultAUC = h2o.nodes[0].generate_auc(thresholds=None, actual=hexKey, predict='Predict.hex', 
                vactual=y, vpredict=1)
            print "AUC result:", h2o.dump_json(resultAUC)

    def test_A_GLM2_basic_predict_prostate(self):
        h2o.nodes[0].log_view()
        namelist = h2o.nodes[0].log_download()
        print "\nStarting prostate.csv"
        # columns start at 0
        y = 1
        csvFilename = "prostate.csv"
        csvPathname = 'logreg/' + csvFilename
        hexKey = csvFilename + ".hex"
        parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, hex_key=hexKey, schema='put')

        for maxx in range(1):
            # 0 is member ID. not used
            # 1 is output
            # original data and description here:
            # http://www.mtech.edu/academics/clsps/math/Data%20Links/benign.txt
            # 
            # SOURCE: The data are from Appendix 5 of
            #   Hosmer, D.W. and Lemeshow, S. (1989) Applied Logistic Regression,
            #   John Wiley and Sons, New York.
            kwargs = {
                'ignored_cols': '0,1',
                'response': y,
                'family': 'binomial',
                'ignored_cols':  '0', 
                'n_folds': 0,
                'alpha': 0,
                'lambda': 1e-8,
            }
            glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=15, **kwargs)
            # ID,CAPSULE,AGE,RACE,DPROS,DCAPS,PSA,VOL,GLEASON
            h2o_glm.simpleCheckGLM(self, glm, 'AGE', **kwargs)
            GLMModel = glm['glm_model']
            modelKey = GLMModel['_key']
            print "Doing predict with same dataset, and the GLM model"
            h2o.nodes[0].generate_predictions(model_key=modelKey, data_key=hexKey, prediction='Predict.hex')

            # just get a predict and AUC on the same data. has to be binomial result
            resultAUC = h2o.nodes[0].generate_auc(thresholds=None, actual=hexKey, predict='Predict.hex', 
                vactual=y, vpredict=1)
            print "AUC result:", h2o.dump_json(resultAUC)

        h2o.nodes[0].log_view()
        namelist = h2o.nodes[0].log_download()

if __name__ == '__main__':
    h2o.unit_main()
