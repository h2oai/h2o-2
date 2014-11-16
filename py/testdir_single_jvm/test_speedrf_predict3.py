import unittest, time, sys, csv
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_import as h2i, h2o_exec as h2e, h2o_rf


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
    return rowNum, predictOutput


class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init(1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_rf_predict3_fvec(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()

        timeoutSecs = 600
        predictHexKey = 'predict_0.hex'
        predictCsv = 'predict_0.csv'
        actualCsv = 'actual_0.csv'

        if 1==1:
            y = 4 # last col
            response = 'response'
            skipSrcOutputHeader = 1
            skipPredictHeader = 1
            trees = 40
            bucket = 'smalldata'
            csvPathname = 'iris/iris2.csv'
            hexKey = 'iris2.csv.hex'
            # translate = {'setosa': 0.0, 'versicolor': 1.0, 'virginica': 2.0}
            # No translate because we're using an Exec to get the data out?, and that loses the encoding?
            translate = None
            # one wrong will be 0.66667. I guess with random, that can happen?
            expectedPctWrong = 0.7

        elif 1==0:
            y = 54 # last col
            response = 'C55'
            skipSrcOutputHeader = 1
            skipPredictHeader = 1
            trees = 6
            # try smaller data set compared to covtype
            bucket = 'home-0xdiag-datasets'
            csvPathname = 'standard/covtype.shuffled.10pct.data'
            hexKey = 'covtype.shuffled.10pct.data.hex'
            translate = {'1': 1, '2': 2, '3': 3, '4': 4, '5': 5, '6': 6, '7': 7}
            expectedPctWrong = 0.7
        elif 1==0:
            y = 54 # last col
            response = 'C55'
            skipSrcOutputHeader = 1
            skipPredictHeader = 1
            trees = 40
            # try smaller data set compared to covtype
            bucket = 'home-0xdiag-datasets'
            csvPathname = 'standard/covtype.shuffled.10pct.data'
            hexKey = 'covtype.shuffled.10pct.data.hex'
            # translate = {1: 0.0, 2: 1.0, 3: 1.0, 4: 1.0, 5: 1.0, 6: 1.0, 7: 1.0}
            translate = {'1': 1, '2': 2, '3': 3, '4': 4, '5': 5, '6': 6, '7': 7}
            expectedPctWrong = 0.7
        elif 1==0:
            y = 54 # last col
            response = 'C55'
            skipSrcOutputHeader = 1
            skipPredictHeader = 1
            trees = 6
            bucket = 'home-0xdiag-datasets'
            csvPathname = 'standard/covtype.data'
            hexKey = 'covtype.data.hex'
            translate = {'1': 1, '2': 2, '3': 3, '4': 4, '5': 5, '6': 6, '7': 7}
            expectedPctWrong = 0.7
        else:
            y = 0 # first col
            response = 'C1'
            skipSrcOutputHeader = 1
            skipPredictHeader = 1
            trees = 6
            bucket = 'home-0xdiag-datasets'
            csvPathname = 'mnist/mnist_training.csv.gz'
            hexKey = 'mnist_training.hex'
            translate = { \
                '0': 0, '1': 1, '2': 2, '3': 3, '4': 4, \
                '5': 5, '6': 6, '7': 7, '8': 8, '9': 9 }
            expectedPctWrong = 0.7

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
            # I looked at what h2o can do for modelling with binomial and it should get better than 25% error?
            if pctWrong > 2.0:
                raise Exception("pctWrong too high. Expect < 2% error because it's reusing training data")
            return pctWrong

        #*****************************************************************************

        parseResult = h2i.import_parse(bucket=bucket, path=csvPathname, schema='put', hex_key=hexKey)
        kwargs = {
            'destination_key': 'rf_model',
            'response': response,
            'ntrees': trees,
            'classification': 1,
            }


        rfResult = h2o_cmd.runSpeeDRF(parseResult=parseResult, timeoutSecs=timeoutSecs, **kwargs)
        rfResult["drf_model"] = rfResult.pop("speedrf_model")
        (classification_error, classErrorPctList, totalScores) = h2o_rf.simpleCheckRFView(rfv=rfResult)

        print "Use H2O GeneratePredictionsPage with a H2O generated model and the same data key."
        print "Does this work? (feeding in same data key)if you're predicting, "
        print "don't you need one less column (the last is output?)"
        print "WARNING: max_iter set to 8 for benchmark comparisons"
        print "y=", y
        pctWrong = predict_and_compare_csvs(model_key='rf_model', hex_key=hexKey, translate=translate, y=y)

        # we are predicting using training data...so error is really low
        # self.assertAlmostEqual(pctWrong, classification_error, delta = 0.2, 
        #     msg="predicted pctWrong: %s should be close to training classification error %s" % (pctWrong, classification_error))
        # can be zero if memorized (iris is either 0 or 0.667?)
        # just make delta 0.7 for now
        self.assertAlmostEqual(pctWrong, expectedPctWrong, delta = 0.7,
                               msg="predicted pctWrong: %s should be small because we're predicting with training data" % pctWrong)


if __name__ == '__main__':
    h2o.unit_main()
