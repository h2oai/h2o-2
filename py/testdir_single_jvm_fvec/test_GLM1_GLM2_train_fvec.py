import unittest, time, sys, csv 
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_import as h2i, h2o_glm, h2o_exec as h2e, h2o_util

print "Comparing GLM1 and GLM2 on covtype, with different alpha/lamba combinations"
print "Will also compare predicts, but having gotten that far without miscompare on training"
MAX_ITER= 50
# 'lsm_solver': [None, 'AUTO','ADMM','GenGradient'],
LSM_SOLVER = 'GenGradient'
LSM_SOLVER = 'ADMM'
LSM_SOLVER = None

# too little
TRY_ALPHA = 0
TRY_LAMBDA = 1e-12
# ok
TRY_ALPHA = 0
TRY_LAMBDA = 1e-8
FAMILY = 'binomial'

STANDARDIZE = 0
BETA_EPSILON = 1e-3

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(node_count=1)
        else:
            h2o_hosts.build_cloud_with_hosts(node_count=1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GLM1_GLM2_train_pred_fvec(self):
        h2o.beta_features = False
        SYNDATASETS_DIR = h2o.make_syn_dir()

        trees = 15
        timeoutSecs = 120

        if 1==0:
            bucket = 'home-0xdiag-datasets'
            csvPathname = 'standard/covtype.data'
            hexKey = 'covtype.data.hex'
            y = 54
        
        if 1==1:
            bucket = 'home-0xdiag-datasets'
            csvPathname = 'standard/covtype.shuffled.10pct.data'
            hexKey = 'covtype.shuffled.10pct.data.hex'
            y = 54

        if 1==0:
            bucket = 'smalldata'
            # no header
            csvPathname = 'iris/iris.csv'
            y = 4
            

        csvFullname = h2i.find_folder_and_filename(bucket, csvPathname, schema='put', returnFullPath=True)
        parseResult = h2i.import_parse(bucket=bucket, path=csvPathname, schema='put', hex_key=hexKey)
        h2o_cmd.runSummary(key=hexKey)

        # do the binomial conversion with Exec2, for both training and test (h2o won't work otherwise)
        trainKey = parseResult['destination_key']

        # just to check. are there any NA/constant cols?
        ignore_x = h2o_glm.goodXFromColumnInfo(y, key=parseResult['destination_key'], timeoutSecs=300)

        #**************************************************************************
        # first glm1
        h2o.beta_features = False
        CLASS = 1
        # try ignoring the constant col to see if it makes a diff
        kwargs = {
            'lsm_solver': LSM_SOLVER,
            'standardize': STANDARDIZE,
            'case': CLASS,
            'case_mode': '=',
            # 'y': 'C' + str(y),
            'y': 'C' + str(y+1),
            'family': FAMILY,
            'n_folds': 1,
            'max_iter': MAX_ITER,
            'beta_epsilon': BETA_EPSILON}

        timeoutSecs = 120
        kwargs.update({'alpha': TRY_ALPHA, 'lambda': TRY_LAMBDA})
        # kwargs.update({'alpha': 0.5, 'lambda': 1e-4})
        # bad model (auc=0.5)
        # kwargs.update({'alpha': 0.0, 'lambda': 0.0})
        start = time.time()
        glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=timeoutSecs, **kwargs)
        # hack. fix bad 'family' ('link' is bad too)..so h2o_glm.py works right
        glm['GLMModel']['GLMParams']['family'] = FAMILY
        print "glm1 end on ", csvPathname, 'took', time.time() - start, 'seconds'
        (warnings, coefficients1, intercept1) = h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)
        iterations1 = glm['GLMModel']['iterations']
        err1 = glm['GLMModel']['validations'][0]['err']
        nullDev1 = glm['GLMModel']['validations'][0]['nullDev']
        resDev1 = glm['GLMModel']['validations'][0]['resDev']

        if FAMILY == 'binomial':
            classErr1 = glm['GLMModel']['validations'][0]['classErr']
            auc1 = glm['GLMModel']['validations'][0]['auc']

        #**************************************************************************
        # then glm2
        h2o.beta_features = True
        kwargs = {
            # 'ignored_cols': 'C29',
            'standardize': STANDARDIZE,
            'classification': 1 if FAMILY=='binomial' else 0,
            # 'response': 'C' + str(y),
            'response': 'C' + str(y+1),
            'family': FAMILY,
            'n_folds': 1,
            'max_iter': MAX_ITER,
            'beta_epsilon': BETA_EPSILON}

        timeoutSecs = 120
        # maybe go back to simpler exec here. this was from when Exec failed unless this was used
        execExpr="B.hex=%s" % trainKey
        h2e.exec_expr(execExpr=execExpr, timeoutSecs=30)
        # class 1=1, all else 0
        if FAMILY == 'binomial':
            execExpr="B.hex[,%s]=(B.hex[,%s]==%s)" % (y+1, y+1, CLASS)
            h2e.exec_expr(execExpr=execExpr, timeoutSecs=30)
        bHack = {'destination_key': 'B.hex'}
        kwargs.update({'alpha': TRY_ALPHA, 'lambda': TRY_LAMBDA})

#        kwargs.update({'alpha': 0.0, 'lambda': 0})
        # kwargs.update({'alpha': 0.5, 'lambda': 1e-4})
        # kwargs.update({'alpha': 0.5, 'lambda': 1e-4})
        # bad model (auc=0.5)
        # kwargs.update({'alpha': 0.0, 'lambda': 0.0})
        start = time.time()
        glm = h2o_cmd.runGLM(parseResult=bHack, timeoutSecs=timeoutSecs, **kwargs)
        print "glm2 end on ", csvPathname, 'took', time.time() - start, 'seconds'
        (warnings, coefficients, intercept) = h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)

        #**************************************************************************

        modelKey = glm['glm_model']['_key']
        avg_err = glm['glm_model']['submodels'][0]['validation']['avg_err']
        best_threshold = glm['glm_model']['submodels'][0]['validation']['best_threshold']
        iteration = glm['glm_model']['submodels'][0]['iteration']
        resDev = glm['glm_model']['submodels'][0]['validation']['residual_deviance']
        nullDev = glm['glm_model']['submodels'][0]['validation']['null_deviance']
        if FAMILY == 'binomial':
            auc = glm['glm_model']['submodels'][0]['validation']['auc']

        self.assertLess(iterations1, MAX_ITER-1, msg="GLM1: Too many iterations, didn't converge %s" % iterations1)
        self.assertLess(iteration, MAX_ITER-1, msg="GLM2: Too many iterations, didn't converge %s" % iteration)

        nullDevExpected = nullDev1
        self.assertAlmostEqual(nullDev, nullDevExpected, delta=2, 
            msg='GLM2 nullDev %s is too different from GLM1 %s' % (nullDev, nullDevExpected))

        iterationExpected = iterations1
        # self.assertAlmostEqual(iteration, iterationExpected, delta=2, 
        #     msg='GLM2 iteration %s is too different from GLM1 %s' % (iteration, iterationExpected))


        # coefficients is a list.
        coeff0 = coefficients[0]
        coeff0Expected = coefficients1[0]
        print "coeff0 pct delta:", "%0.3f" % (100.0 * (abs(coeff0) - abs(coeff0Expected))/abs(coeff0Expected))
        self.assertTrue(h2o_util.approxEqual(coeff0, coeff0Expected, rel=0.20),
            msg='GLM2 coefficient 0 %s is too different from GLM1 %s' % (coeff0, coeff0Expected))

        
        coeff2 = coefficients[2]
        coeff2Expected = coefficients1[2]
        print "coeff2 pct delta:", "%0.3f" % (100.0 * (abs(coeff2) - abs(coeff2Expected))/abs(coeff2Expected))
        self.assertTrue(h2o_util.approxEqual(coeff2, coeff2Expected, rel=0.20),
            msg='GLM2 coefficient 2 %s is too different from GLM1 %s' % (coeff2, coeff2Expected))

        # compare to known values GLM1 got for class 1 case, with these parameters
        # aucExpected = 0.8428
        if FAMILY == 'binomial':
            aucExpected = auc1
            self.assertAlmostEqual(auc, aucExpected, delta=10, 
                msg='GLM2 auc %s is too different from GLM1 %s' % (auc, aucExpected))

        interceptExpected = intercept1
        print "intercept pct delta:", 100.0 * (abs(intercept) - abs(interceptExpected))/abs(interceptExpected)
        self.assertTrue(h2o_util.approxEqual(intercept, interceptExpected, rel=0.20),
            msg='GLM2 intercept %s is too different from GLM1 %s' % (intercept, interceptExpected))

        # avg_errExpected = 0.2463
        avg_errExpected = err1
        self.assertAlmostEqual(avg_err, avg_errExpected, delta=0.50*avg_errExpected, 
            msg='GLM2 avg_err %s is too different from GLM1 %s' % (avg_err, avg_errExpected))

        self.assertAlmostEqual(best_threshold, 0.35, delta=0.10*best_threshold, 
            msg='GLM2 best_threshold %s is too different from GLM1 %s' % (best_threshold, 0.35))

if __name__ == '__main__':
    h2o.unit_main()
