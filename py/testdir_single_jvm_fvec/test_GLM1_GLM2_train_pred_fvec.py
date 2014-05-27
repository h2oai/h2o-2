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
# translate provides the mapping between original and predicted
# since GLM is binomial, We predict 0 for 0 and 1 for > 0
def compare_csv_last_col(csvPathname, msg, translate=None, skipHeader=False):
    predictOutput = []
    with open(csvPathname, 'rb') as f:
        reader = csv.reader(f)
        print "csv read of", csvPathname
        rowNum = 0
        for row in reader:
            # print the last col
            # ignore the first row ..header
            if skipHeader and rowNum==0:
                print "Skipping header in this csv"
            else:
                output = row[-1]
                if translate:
                    output = translate[int(output)]
                # only print first 10 for seeing
                if rowNum<10: print msg, row[-1], output
                predictOutput.append(output)
            rowNum += 1
    return (rowNum, predictOutput)

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
            

        predictHexKey = 'predict.hex'
        predictCsv = 'predict.csv'

        execHexKey = 'A.hex'
        execCsv = 'exec.csv'


        csvPredictPathname = SYNDATASETS_DIR + "/" + predictCsv
        csvExecPathname = SYNDATASETS_DIR + "/" + execCsv
        # for using below in csv reader
        csvFullname = h2i.find_folder_and_filename(bucket, csvPathname, schema='put', returnFullPath=True)

        def predict_and_compare_csvs(model_key):
            start = time.time()
            predict = h2o_cmd.runPredict(model_key=model_key, data_key=hexKey, destination_key=predictHexKey)
            print "runPredict end on ", hexKey, " took", time.time() - start, 'seconds'
            h2o.check_sandbox_for_errors()
            inspect = h2o_cmd.runInspect(key=predictHexKey)
            h2o_cmd.infoFromInspect(inspect, 'predict.hex')

            h2o.nodes[0].csv_download(src_key=predictHexKey, csvPathname=csvPredictPathname)
            h2o.nodes[0].csv_download(src_key=execHexKey, csvPathname=csvExecPathname)
            h2o.check_sandbox_for_errors()

            print "Do a check of the original output col against predicted output"
            translate = {1: 0.0, 2: 1.0, 3: 1.0, 4: 1.0, 5: 1.0, 6: 1.0, 7: 1.0}
            (rowNum1, originalOutput) = compare_csv_last_col(csvExecPathname,
                msg="Original, after being exec'ed", skipHeader=True)
            (rowNum2, predictOutput)  = compare_csv_last_col(csvPredictPathname, 
                msg="Predicted", skipHeader=True)

            # no header on source
            if (rowNum1 != rowNum2):
                raise Exception("original rowNum1: %s not same as downloaded predict (w/header) rowNum2: \
                    %s" % (rowNum1, rowNum2))

            wrong = 0
            wrong0 = 0
            wrong1 = 0
            for rowNum,(o,p) in enumerate(zip(originalOutput, predictOutput)):
                o = float(o)
                p = float(p)
                if o!=p:
                    msg = "Comparing original output col vs predicted. row %s differs. \
                        original: %s predicted: %s"  % (rowNum, o, p)
                    if p==0.0 and wrong0==10:
                        print "Not printing any more predicted=0 mismatches"
                    elif p==0.0 and wrong0<10:
                        print msg
                    if p==1.0 and wrong1==10:
                        print "Not printing any more predicted=1 mismatches"
                    elif p==1.0 and wrong1<10:
                        print msg

                    if p==0.0:
                        wrong0 += 1
                    elif p==1.0:
                        wrong1 += 1

                    wrong += 1

            print "wrong0:", wrong0
            print "wrong1:", wrong1
            print "\nTotal wrong:", wrong
            print "Total:", len(originalOutput)
            pctWrong = (100.0 * wrong)/len(originalOutput)
            print "wrong/Total * 100 ", pctWrong
            # I looked at what h2o can do for modelling with binomial and it should get better than 25% error?
            if pctWrong > 16.0:
                raise Exception("pct wrong: %s too high. Expect < 16 pct error" % pctWrong)

        #*************************************************************************
        parseResult = h2i.import_parse(bucket=bucket, path=csvPathname, schema='put', hex_key=hexKey)
        h2o_cmd.runSummary(key=hexKey)

        # do the binomial conversion with Exec2, for both training and test (h2o won't work otherwise)
        trainKey = parseResult['destination_key']

        # just to check. are there any NA/constant cols?
        ignore_x = h2o_glm.goodXFromColumnInfo(y, key=parseResult['destination_key'], timeoutSecs=300)

        #**************************************************************************
        # first glm1
        h2o.beta_features = False
        # try ignoring the constant col to see if it makes a diff
        kwargs = {
            'lsm_solver': LSM_SOLVER,
            'standardize': STANDARDIZE,
            # 'y': 'C' + str(y),
            'y': 'C' + str(y+1),
            'family': FAMILY,
            'n_folds': 1,
            'max_iter': MAX_ITER,
            'beta_epsilon': BETA_EPSILON}

        CLASS=1
        # maybe go back to simpler exec here. this was from when Exec failed unless this was used
        execExpr="A.hex=%s" % parseResult['destination_key']
        h2e.exec_expr(execExpr=execExpr, timeoutSecs=30)
        # class 1=1, all else 0
        if FAMILY == 'binomial':
            execExpr="A.hex[,%s]=(A.hex[,%s]==%s)" % (y+1, y+1, CLASS)
            h2e.exec_expr(execExpr=execExpr, timeoutSecs=30)
        aHack = {'destination_key': 'A.hex'}
        
        timeoutSecs = 120
        kwargs.update({'alpha': TRY_ALPHA, 'lambda': TRY_LAMBDA})
        # kwargs.update({'alpha': 0.5, 'lambda': 1e-4})
        # bad model (auc=0.5)
        # kwargs.update({'alpha': 0.0, 'lambda': 0.0})
        start = time.time()
        glm = h2o_cmd.runGLM(parseResult=aHack, timeoutSecs=timeoutSecs, **kwargs)
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
        self.assertAlmostEqual(iteration, iterationExpected, delta=2, 
            msg='GLM2 iteration %s is too different from GLM1 %s' % (iteration, iterationExpected))


        # coefficients is a list.
        coeff0 = coefficients[0]
        coeff0Expected = coefficients1[0]
        print "coeff0 pct delta:", "%0.3f" % (100.0 * (abs(coeff0) - abs(coeff0Expected))/abs(coeff0Expected))
        self.assertTrue(h2o_util.fp_approx_equal(coeff0, coeff0Expected, rel=0.01),
            msg='GLM2 coefficient 0 %s is too different from GLM1 %s' % (coeff0, coeff0Expected))

        
        coeff2 = coefficients[2]
        coeff2Expected = coefficients1[2]
        print "coeff2 pct delta:", "%0.3f" % (100.0 * (abs(coeff2) - abs(coeff2Expected))/abs(coeff2Expected))
        self.assertTrue(h2o_util.fp_approx_equal(coeff2, coeff2Expected, rel=0.01),
            msg='GLM2 coefficient 2 %s is too different from GLM1 %s' % (coeff2, coeff2Expected))

        # compare to known values GLM1 got for class 1 case, with these parameters
        # aucExpected = 0.8428
        if FAMILY == 'binomial':
            aucExpected = auc1
            self.assertAlmostEqual(auc, aucExpected, delta=10, 
                msg='GLM2 auc %s is too different from GLM1 %s' % (auc, aucExpected))

        interceptExpected = intercept1
        print "intercept pct delta:", 100.0 * (abs(intercept) - abs(interceptExpected))/abs(interceptExpected)
        self.assertTrue(h2o_util.fp_approx_equal(intercept, interceptExpected, rel=0.01),
            msg='GLM2 intercept %s is too different from GLM1 %s' % (intercept, interceptExpected))

        # avg_errExpected = 0.2463
        avg_errExpected = err1
        self.assertAlmostEqual(avg_err, avg_errExpected, delta=0.05*avg_errExpected, 
            msg='GLM2 avg_err %s is too different from GLM1 %s' % (avg_err, avg_errExpected))

        self.assertAlmostEqual(best_threshold, 0.35, delta=0.01*best_threshold, 
            msg='GLM2 best_threshold %s is too different from GLM1 %s' % (best_threshold, 0.35))

        predict_and_compare_csvs(model_key=modelKey)

if __name__ == '__main__':
    h2o.unit_main()
