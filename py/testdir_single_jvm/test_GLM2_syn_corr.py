import unittest, random, sys, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i, h2o_glm, h2o_util, h2o_jobs, h2o_gbm, h2o_exec as h2e

DO_BUG = False
DO_HDFS = False
DO_ALL_DIGITS = False

print "Uses numpy to create dataset..I guess we have to deal with jenkins not having it"
print "uses dot product off some coefficients to create output. also correlatation with constant term in cols"
SCIPY_INSTALLED = True
try:
    import scipy as sp
    import numpy as np
    print "Both numpy and scipy are installed. Will do extra checks"

except ImportError:
    print "numpy or scipy is not installed. Will only do sort-based checking"
    SCIPY_INSTALLED = False

def write_syn_dataset(csvPathname, rowCount=100, colCount=10):
    # http://nbviewer.ipython.org/github/fabianp/pytron/blob/master/doc/benchmark_logistic.ipynb
    # http://fa.bianp.net/blog/2013/numerical-optimizers-for-logistic-regression/

    # The synthetic data used in the benchmarks was generated as described in 2 and
    # consists primarily of the design matrix X being Gaussian noise,
    # the vector of coefficients is drawn also from a Gaussian distribution
    # and the explained variable y is generated as y=sign(Xw).
    # We then perturb matrix X by adding Gaussian noise with covariance 0.8.

    corr = 1. # 0., 1., 10.
    n_samples = rowCount
    n_features = colCount
    np.random.seed(0)

    X = np.random.randn(n_samples, n_features)
    w = np.random.randn(n_features)

    # np.sign returns sign
    y = np.sign(X.dot(w))
    X += 0.8 * np.random.randn(n_samples, n_features) # add noise
    X+= corr # this makes it correlated by adding a constant term
    # X = np.hstack((X, np.ones((X.shape[0], 1)))) # add a column of ones for intercept

    print X.shape
    print y.shape
    # concatenate X and y columns together so we can write a csv
    y2 = np.reshape(y, (X.shape[0], 1))
    Xy = np.hstack((X, y2))
    np.savetxt(csvPathname, Xy, delimiter=',', fmt='%5.4f')

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        # assume we're at 0xdata with it's hdfs namenode
        h2o.init(1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GLM2_mnist(self):
        if not SCIPY_INSTALLED:
            pass

        else:    
            SYNDATASETS_DIR = h2o.make_syn_dir()

            csvFilelist = [
                (10000, 500, 'cA', 60),
            ]

            trial = 0
            for (rowCount, colCount, hex_key, timeoutSecs) in csvFilelist:
                trialStart = time.time()

                # PARSE test****************************************
                csvFilename = 'syn_' + "binary" + "_" + str(rowCount) + 'x' + str(colCount) + '.csv'
                csvPathname = SYNDATASETS_DIR + "/" + csvFilename
                write_syn_dataset(csvPathname, rowCount, colCount)

                start = time.time()
                parseResult = h2i.import_parse(path=csvPathname, schema='put', 
                    hex_key=hex_key, timeoutSecs=timeoutSecs)
                elapsed = time.time() - start
                print "parse end on ", csvFilename, 'took', elapsed, 'seconds',\
                    "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)

                # GLM****************************************
                modelKey = 'GLM_model'
                y = colCount 
                kwargs = {
                    'response': 'C' + str(y+1),
                    'family': 'binomial',
                    'lambda': 1e-4, 
                    'alpha': 0,
                    'max_iter': 15,
                    'n_folds': 1,
                    'beta_epsilon': 1.0E-4,
                    'destination_key': modelKey,
                    }

                # GLM wants the output col to be strictly 0,1 integer
                execExpr = "aHack=%s; aHack[,%s] = aHack[,%s]==1" % (hex_key, y+1, y+1)
                h2e.exec_expr(execExpr=execExpr, timeoutSecs=30)
                aHack = {'destination_key': 'aHack'}

                
                timeoutSecs = 1800
                start = time.time()
                glm = h2o_cmd.runGLM(parseResult=aHack, timeoutSecs=timeoutSecs, pollTimeoutSecs=60, **kwargs)
                elapsed = time.time() - start
                print "GLM completed in", elapsed, "seconds.", \
                    "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)

                h2o_glm.simpleCheckGLM(self, glm, None, noPrint=True, **kwargs)
                modelKey = glm['glm_model']['_key']

                # This seems wrong..what's the format of the cm?
                lambdaMax = glm['glm_model']['lambda_max']
                print "lambdaMax:", lambdaMax

                best_threshold= glm['glm_model']['submodels'][0]['validation']['best_threshold']
                print "best_threshold", best_threshold

                # pick the middle one?
                cm = glm['glm_model']['submodels'][0]['validation']['_cms'][5]['_arr']
                print "cm:", cm
                pctWrong = h2o_gbm.pp_cm_summary(cm);
                # self.assertLess(pctWrong, 9,"Should see less than 9% error (class = 4)")

                print "\nTrain\n==========\n"
                print h2o_gbm.pp_cm(cm)

                # Score *******************************
                # this messes up if you use case_mode/case_vale above
                print "\nPredict\n==========\n"
                predictKey = 'Predict.hex'
                start = time.time()

                predictResult = h2o_cmd.runPredict(
                    data_key='aHack',
                    model_key=modelKey,
                    destination_key=predictKey,
                    timeoutSecs=timeoutSecs)

                predictCMResult = h2o.nodes[0].predict_confusion_matrix(
                    actual='aHack',
                    vactual='C' + str(y+1),
                    predict=predictKey,
                    vpredict='predict',
                    )

                cm = predictCMResult['cm']

                # These will move into the h2o_gbm.py
                pctWrong = h2o_gbm.pp_cm_summary(cm);
                self.assertLess(pctWrong, 50,"Should see less than 50% error")

                print "\nTest\n==========\n"
                print h2o_gbm.pp_cm(cm)


if __name__ == '__main__':
    h2o.unit_main()
