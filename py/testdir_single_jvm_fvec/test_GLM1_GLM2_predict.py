import unittest, time, sys, csv 
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_import as h2i, h2o_glm, h2o_exec as h2e, h2o_util
import h2o_browse as h2b

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
TRY_LAMBDA = 0
FAMILY = 'binomial'

STANDARDIZE = 0
BETA_EPSILON = 1e-3

# translate provides the mapping between original and predicted
def compare_csv_at_one_col(csvPathname, msg, colIndex=-1, translate=None, skipHeader=0):
    predictOutput = []
    with open(csvPathname, 'rb') as f:
        reader = csv.reader(f)
        print "csv read of", csvPathname, "column", colIndex
        rowNum = 0
        for row in reader:
            # print the last col
            # ignore the first row ..header
            if skipHeader==1 and rowNum==0:
                print "Skipping header in this csv"
            else:
                output = row[colIndex]
                if translate:
                    output = translate[output]
                # only print first 10 for seeing
                if rowNum<10: print msg, "raw output col:", row[colIndex], "translated:", output
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
        # h2b.browseTheCloud()
        h2o.beta_features = False
        SYNDATASETS_DIR = h2o.make_syn_dir()

        trees = 15
        timeoutSecs = 120
        predictHexKey = 'predict_0.hex'
        predictCsv = 'predict_0.csv'
        actualCsv = 'actual_0.csv'

        if 1==0:
            skipSrcOutputHeader = 1
            skipPredictHeader = 1
            bucket = 'home-0xdiag-datasets'
            csvPathname = 'standard/covtype.data'
            hexKey = 'covtype.data.hex'
            y = 54
            expectedPctWrong = 0
        
        if 1==0:
            skipSrcOutputHeader = 1
            skipPredictHeader = 1
            bucket = 'home-0xdiag-datasets'
            csvPathname = 'standard/covtype.shuffled.10pct.data'
            hexKey = 'covtype.shuffled.10pct.data.hex'
            y = 54
            expectedPctWrong = 0

        if 1==1:
            skipSrcOutputHeader = 1
            skipPredictHeader = 1
            bucket = 'smalldata'
            # no header
            csvPathname = 'iris/iris.csv'
            hexKey = 'iris.hex'
            y = 4
            expectedPctWrong = 26
            

        csvPredictPathname = SYNDATASETS_DIR + "/" + predictCsv
        csvSrcOutputPathname = SYNDATASETS_DIR + "/" + actualCsv
        # for using below in csv reader
        csvFullname = h2i.find_folder_and_filename(bucket, csvPathname, schema='put', returnFullPath=True)
        def predict_and_compare_csvs(model_key, hex_key, translate=None, y=0):
            # have to slice out col 0 (the output) and feed result to predict
            # cols are 0:784 (1 output plus 784 input features
            # h2e.exec_expr(execExpr="P.hex="+hex_key+"[1:784]", timeoutSecs=30)
            dataKey = "P.hex"
            h2e.exec_expr(execExpr=dataKey+"="+hex_key, timeoutSecs=30) # unneeded but interesting
            if skipSrcOutputHeader:
                print "Has header in dataset, so should be able to chop out col 0 for predict and get right answer"
                print "hack for now, can't chop out col 0 in Exec currently"
                dataKey = hex_key
            else:
                print "No header in dataset, can't chop out cols, since col numbers are used for names"
                dataKey = hex_key

            # +1 col index because R-like
            h2e.exec_expr(execExpr="Z.hex="+hex_key+"[,"+str(y+1)+"]", timeoutSecs=30)

            start = time.time()
            predict = h2o.nodes[0].generate_predictions(model_key=model_key,
                data_key=hexKey, destination_key=predictHexKey)
            print "generate_predictions end on ", hexKey, " took", time.time() - start, 'seconds'
            h2o.check_sandbox_for_errors()
            inspect = h2o_cmd.runInspect(key=predictHexKey)
            h2o_cmd.infoFromInspect(inspect, 'predict.hex')

            h2o.nodes[0].csv_download(src_key="Z.hex", csvPathname=csvSrcOutputPathname)
            h2o.nodes[0].csv_download(src_key=predictHexKey, csvPathname=csvPredictPathname)
            h2o.check_sandbox_for_errors()

            print "Do a check of the original output col against predicted output"
            (rowNum1, originalOutput) = compare_csv_at_one_col(csvSrcOutputPathname,
                msg="Original", colIndex=0, translate=translate, skipHeader=skipSrcOutputHeader)
            (rowNum2, predictOutput)  = compare_csv_at_one_col(csvPredictPathname,
                msg="Predicted", colIndex=0, skipHeader=skipPredictHeader)

            # no header on source
            if ((rowNum1-skipSrcOutputHeader) != (rowNum2-skipPredictHeader)):
                raise Exception("original rowNum1: %s - %d not same as downloaded predict: rowNum2: %s - %d \
                    %s" % (rowNum1, skipSrcOutputHeader, rowNum2, skipPredictHeader))

            wrong = 0
            for rowNum,(o,p) in enumerate(zip(originalOutput, predictOutput)):
                # if float(o)!=float(p):
                if str(o)!=str(p):
                    if wrong==10:
                        print "Not printing any more mismatches\n"
                    elif wrong<10:
                        msg = "Comparing original output col vs predicted. row %s differs. \
                            original: %s predicted: %s"  % (rowNum, o, p)
                        print msg
                    wrong += 1

            print "\nTotal wrong:", wrong
            print "Total:", len(originalOutput)
            pctWrong = (100.0 * wrong)/len(originalOutput)
            print "wrong/Total * 100 ", pctWrong
            return pctWrong


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
            'y': 'C' + str(y+1),
            'family': FAMILY,
            'n_folds': 0,
            'max_iter': MAX_ITER,
            'beta_epsilon': BETA_EPSILON,
            'case': CLASS,
            'case_mode': '=',
         }

        timeoutSecs = 120
        kwargs.update({'alpha': TRY_ALPHA, 'lambda': TRY_LAMBDA})
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
            'response': 'C' + str(y+1),
            'family': FAMILY,
            'n_folds': 0,
            'max_iter': MAX_ITER,
            'beta_epsilon': BETA_EPSILON}

        timeoutSecs = 120

        # class 1=1, all else 0
        if FAMILY == 'binomial':
            execExpr="B.hex=%s; B.hex[,%s]=(%s[,%s]==%s)" % (trainKey, y+1, trainKey, y+1, CLASS)
            h2e.exec_expr(execExpr=execExpr, timeoutSecs=30)
            bHack = {'destination_key': 'B.hex'}
        else:
            bHack = parseResult
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
        self.assertTrue(h2o_util.approxEqual(coeff0, coeff0Expected, rel=0.5),
            msg='GLM2 coefficient 0 %s is too different from GLM1 %s' % (coeff0, coeff0Expected))

        
        coeff2 = coefficients[2]
        coeff2Expected = coefficients1[2]
        print "coeff2 pct delta:", "%0.3f" % (100.0 * (abs(coeff2) - abs(coeff2Expected))/abs(coeff2Expected))
        self.assertTrue(h2o_util.approxEqual(coeff2, coeff2Expected, rel=0.5),
            msg='GLM2 coefficient 2 %s is too different from GLM1 %s' % (coeff2, coeff2Expected))

        # compare to known values GLM1 got for class 1 case, with these parameters
        # aucExpected = 0.8428
        if FAMILY == 'binomial':
            aucExpected = auc1
            self.assertAlmostEqual(auc, aucExpected, delta=10, 
                msg='GLM2 auc %s is too different from GLM1 %s' % (auc, aucExpected))

        interceptExpected = intercept1
        print "intercept pct delta:", 100.0 * (abs(intercept) - abs(interceptExpected))/abs(interceptExpected)
        self.assertTrue(h2o_util.approxEqual(intercept, interceptExpected, rel=0.5),
            msg='GLM2 intercept %s is too different from GLM1 %s' % (intercept, interceptExpected))


        # avg_errExpected = 0.2463
        avg_errExpected = err1
        self.assertAlmostEqual(avg_err, avg_errExpected, delta=0.50*avg_errExpected, 
            msg='GLM2 avg_err %s is too different from GLM1 %s' % (avg_err, avg_errExpected))

        # self.assertAlmostEqual(best_threshold, 0.35, delta=0.10*best_threshold, 
        #     msg='GLM2 best_threshold %s is too different from GLM1 %s' % (best_threshold, 0.35))

        #********************
        # Print comparison
        #********************
        interceptDelta = abs(abs(intercept1) - abs(intercept))
        cDelta = [abs(abs(a) - abs(b)) for a,b in zip(coefficients1, coefficients)]

        def printit(self, a, b, c, d):
            pctDiff = abs(d/c)*100
            print "%-20s %-20.5e %8s %5.2f%% %10s %-20.5e" % \
                ("GLM2: " + a + " " + b + ":", c, "pct. diff:", pctDiff, "abs diff:", d)
            # self.assertLess(pctDiff,1,"Expect <1% difference between H2O and R coefficient/intercept")

        printit(self, "intercept", "", intercept1, interceptDelta)
        print "compare lengths coefficients1, coefficients, cDelta:", len(coefficients1), len(coefficients), len(cDelta)
        print "GLM1:", coefficients1
        print "GLM2:", coefficients
        print "cDelta:", cDelta

        for i,cValue in enumerate(coefficients):
            printit(self , "coefficient", "C"+str(i), cValue, cDelta[i])

        hexKey = 'B.hex'
        pctWrong = predict_and_compare_csvs(model_key=modelKey, hex_key=hexKey, translate=None, y=y)

        # we are predicting using training data...so error is really low
        # self.assertAlmostEqual(pctWrong, classification_error, delta = 0.2, 
        #     msg="predicted pctWrong: %s should be close to training classification error %s" % (pctWrong, classification_error))
        self.assertAlmostEqual(pctWrong, expectedPctWrong, delta = 2.0,
            msg="predicted pctWrong: %s should be small because we're predicting with training data %s" % (pctWrong, expectedPctWrong))




if __name__ == '__main__':
    h2o.unit_main()
