import unittest, time, sys, csv
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_import as h2i, h2o_glm, h2o_exec as h2e

# Notes
# 1) all input mappings for training and test (scoring) are locked into the model. 
# # This can be column index if no header is present, or column name if header is present.
# 
# If a input mapping uses a name, then the test (scoring) can have arbitrary arrangement of 
# input cols relative to the training data cols. The mapping is resolved using the  header names. 
# I'm assuming there's no reordering possible without the use of header (named input cols during parse).
# 
# 2) The output mapping for scoring never comes from the model. 
# You have to give it as a parameter, regardless of whether there was a named column or not.
# Now: assuming I built a model with no header in the dataset
# If I have no header on the training data set, and no header on the test data set, 
# and I re-arrange the columns in the test data set, the model will generate the wrong answers. 
# So I am not allowed to do that. The mapping is fixed, and it's in the model.
# 
# So: If all that is correct:
# If I don't have a header in mnist, and I want to predict, the test data has to be in cols 1:784 like the train data.
# And if I had a header, then the test data can be arranged any which way.
# Reusing the training data, exactly as is, for predict, is legal, because col 0 won't be used.


# translate provides the mapping between original and predicted
# since GLM is binomial, We predict 0 for 0 and 1 for > 0
# default to last col

HAS_HEADER = True
def compare_csv_at_one_col(csvPathname, msg, colIndex=-1,translate=None, skipHeader=0):
    predictOutput = []
    with open(csvPathname, 'rb') as f:
        reader = csv.reader(f)
        print "csv read of", csvPathname
        rowNum = 0
        for row in reader:
            # print the last col
            # ignore the first row ..header
            if skipHeader==1 and rowNum==0:
                print "Skipping header in this csv"
            else:
                output = row[colIndex]
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
            h2o.build_cloud(node_count=3, java_heap_GB=4)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_glm_predict3(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()

        trees = 15
        timeoutSecs = 120
        if 1==1:
            csvPathname = 'mnist/mnist_training.csv.gz'
            hexKey = 'mnist.hex'
        else:
            # try smaller data set
            csvPathname = 'mnist/mnist_testing.csv.gz'
            hexKey = 'mnist.hex'

        predictHexKey = 'predict_0.hex'
        predictCsv = 'predict_0.csv'
        actualCsv = 'actual_0.csv'
        bucket = 'home-0xdiag-datasets'

        csvPredictPathname = SYNDATASETS_DIR + "/" + predictCsv
        csvSrcOutputPathname = SYNDATASETS_DIR + "/" + actualCsv
        # for using below in csv reader
        csvFullname = h2i.find_folder_and_filename(bucket, csvPathname, schema='put', returnFullPath=True)


        def predict_and_compare_csvs(model_key, hex_key, translate):
            # have to slice out col 0 (the output) and feed result to predict
            # cols are 0:784 (1 output plus 784 input features
            # h2e.exec_expr(execExpr="P.hex="+hex_key+"[1:784]", timeoutSecs=30)
            dataKey = "P.hex"
            h2e.exec_expr(execExpr=dataKey+"="+hex_key, timeoutSecs=30) # unneeded but interesting
            if HAS_HEADER:
                print "Has header in dataset, so should be able to chop out col 0 for predict and get right answer"
                print "hack for now, can't chop out col 0 in Exec currently"
                dataKey = hex_key
            else:
                print "No header in dataset, can't chop out cols, since col numbers are used for names"
                dataKey = hex_key
            
            h2e.exec_expr(execExpr="Z.hex="+hex_key+"[0]", timeoutSecs=30)
            start = time.time()
            # predict = h2o.nodes[0].generate_predictions(model_key=model_key, data_key="P.hex",
            predict = h2o.nodes[0].generate_predictions(model_key=model_key, data_key=dataKey,
                destination_key=predictHexKey)
            print "generate_predictions end on ", hexKey, " took", time.time() - start, 'seconds'
            h2o.check_sandbox_for_errors()
            inspect = h2o_cmd.runInspect(key=predictHexKey)
            h2o_cmd.infoFromInspect(inspect, 'predict.hex')

            h2o.nodes[0].csv_download(src_key="Z.hex", csvPathname=csvSrcOutputPathname)
            h2o.nodes[0].csv_download(src_key=predictHexKey, csvPathname=csvPredictPathname)
            h2o.check_sandbox_for_errors()

            # depending what you use, may need to set these to 0 or 1
            skipSrcOutputHeader = 1
            skipPredictHeader= 1
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
            # digit 3 with no regularization got around 5%. 0 gets < 2%
            if pctWrong > 6.0:
                raise Exception("pct wrong too high. Expect < 6% error")

        #*************************************************************************
        parseResult = h2i.import_parse(bucket=bucket, path=csvPathname, schema='put', hex_key=hexKey)

        print "Use H2O GeneratePredictionsPage with a H2O generated model and the same data key." 
        print "Does this work? (feeding in same data key)if you're predicting, "
        print "don't you need one less column (the last is output?)"
        print "WARNING: max_iter set to 8 for benchmark comparisons"

        # excessive, trying each digit one at a time, but hey a good test
        # limit the ierations so it doesn't take so long. 4 minutes per digit?
        max_iter = 4
        # for y in [0,1,2,3,4,5,6,7,8,9]:
        # for y in [0,3,7]:
        for case in [0,3,7]:

            translate = {}
            for i in range(10):
                if i == case: 
                    translate[i] = 1.0
                else:
                    translate[i] = 0.0

            print "translate:", translate
            kwargs = {
                'x': "",
                'y': 0,
                'family': 'binomial',
                'link': 'logit',
                'n_folds': 1,
                'case_mode': '=',
                'case': case, # zero should predict to 1, 2-9 should predict to 0
                'max_iter': max_iter,
                'beta_epsilon': 1e-3}

            timeoutSecs = 120


            # L2 
            start = time.time()
            kwargs.update({'alpha': 0, 'lambda': 0})
            glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=timeoutSecs, **kwargs)
            print "glm (L2) end on ", csvPathname, 'took', time.time() - start, 'seconds'
            h2o_glm.simpleCheckGLM(self, glm, 13, **kwargs)
            predict_and_compare_csvs(model_key=glm['destination_key'], hex_key=hexKey, translate=translate)

            # Elastic
            kwargs.update({'alpha': 0.5, 'lambda': 1e-4})
            start = time.time()
            glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=timeoutSecs, **kwargs)
            print "glm (Elastic) end on ", csvPathname, 'took', time.time() - start, 'seconds'
            # okay for some coefficients to go to zero!
            h2o_glm.simpleCheckGLM(self, glm, 13, allowZeroCoeff=True, **kwargs)
            predict_and_compare_csvs(model_key=glm['destination_key'], hex_key=hexKey, translate=translate)

            # L1
            kwargs.update({'alpha': 1, 'lambda': 1e-4})
            start = time.time()
            glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=timeoutSecs, **kwargs)
            print "glm (L1) end on ", csvPathname, 'took', time.time() - start, 'seconds'
            # okay for some coefficients to go to zero!
            h2o_glm.simpleCheckGLM(self, glm, 13, allowZeroCoeff=True, **kwargs)
            predict_and_compare_csvs(model_key=glm['destination_key'], hex_key=hexKey, translate=translate)

if __name__ == '__main__':
    h2o.unit_main()
