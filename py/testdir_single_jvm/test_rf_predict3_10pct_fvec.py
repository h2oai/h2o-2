import unittest, time, sys, csv
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_import as h2i, h2o_rf

# translate provides the mapping between original and predicted
# last col is -1
def compare_csv(csvPathname, msg, translate=None, skipHeader=False, col=-1):
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
                # output = row[-1]
                # v2
                output = row[col]
                # print "output:", output
                # print "row:", row
                if translate:
                    output = translate[output]
                # only print first 10 for seeing
                # if rowNum<10: print msg, "raw output col:", row[-1], "translated:", output
                # v2
                if rowNum<10: print msg, "raw output col:", row[col], "translated:", output
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
            h2o.build_cloud(1)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_rf_predict3_10pct_fvec(self):
        h2o.beta_features = True
        SYNDATASETS_DIR = h2o.make_syn_dir()

        timeoutSecs = 60
        predictHexKey = 'predict.hex'
        predictCsv = 'predict.csv'

        if 1==0:
            skipSrcHeader = True
            trees = 6
            bucket = 'smalldata'
            csvPathname = 'iris/iris2.csv'
            hexKey = 'iris2.csv.hex'
            translate = {'setosa': 0.0, 'versicolor': 1.0, 'virginica': 2.0}
            # new for v2.0...no translate needed?
            translate = {'setosa': 'setosa', 'versicolor': 'versicolor', 'virginica': 'virginica'}
        elif 1==1:
            skipSrcHeader = False
            trees = 6
            # try smaller data set compared to covtype
            bucket = 'home-0xdiag-datasets'
            csvPathname = 'standard/covtype.shuffled.10pct.data'
            hexKey = 'covtype.shuffled.10pct.data.hex'
            # translate = {1: 0.0, 2: 1.0, 3: 1.0, 4: 1.0, 5: 1.0, 6: 1.0, 7: 1.0}
            translate = {'1': 1, '2': 2, '3': 3, '4': 4, '5': 5, '6': 6, '7': 7}
        else:
            skipSrcHeader = False
            trees = 6
            bucket = 'home-0xdiag-datasets'
            csvPathname = 'standard/covtype.data'
            hexKey = 'covtype.data.hex'
            # translate = {1: 0.0, 2: 1.0, 3: 1.0, 4: 1.0, 5: 1.0, 6: 1.0, 7: 1.0}
            translate = {'1': 1, '2': 2, '3': 3, '4': 4, '5': 5, '6': 6, '7': 7}


        csvPredictPathname = SYNDATASETS_DIR + "/" + predictCsv
        # for using below in csv reader
        csvFullname = h2i.find_folder_and_filename(bucket, csvPathname, schema='put', returnFullPath=True)

        def predict_and_compare_csvs(model_key, translate=None):
            start = time.time()
            predict = h2o.nodes[0].generate_predictions(model_key=model_key,
                data_key=hexKey, destination_key=predictHexKey)
            print "generate_predictions end on ", hexKey, " took", time.time() - start, 'seconds'
            h2o.check_sandbox_for_errors()
            inspect = h2o_cmd.runInspect(key=predictHexKey)
            h2o_cmd.infoFromInspect(inspect, 'predict.hex')

            h2o.nodes[0].csv_download(src_key=predictHexKey, csvPathname=csvPredictPathname)
            h2o.check_sandbox_for_errors()

            print "Do a check of the original output col against predicted output"
            (rowNum1, originalOutput) = compare_csv(csvFullname, col=-1,
                msg="Original", translate=translate, skipHeader=skipSrcHeader)
            (rowNum2, predictOutput)  = compare_csv(csvPredictPathname,  col=0,
                msg="Predicted", skipHeader=True)

            # both source and predict have headers, so no expected mismatch?
            expHeaderMismatch = 0 if skipSrcHeader else 1
            if ((rowNum1+expHeaderMismatch) != rowNum2):
                raise Exception("original rowNum1: %s + %s not same as downloaded predict rowNum2: %s" \
                % (rowNum1, expHeaderMismatch, rowNum2))

            wrong = 0
            for rowNum,(o,p) in enumerate(zip(originalOutput, predictOutput)):
                # if float(o)!=float(p):
                # v2
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
            print "pctWrong: (wrong/Total * 100) = ", pctWrong
            # I looked at what h2o can do for modelling with binomial and it should get better than 25% error?
            if pctWrong > 2.0:
                raise Exception("pct wrong too high. Expect < 2% error because it's reusing training data")

        #*****************************************************************************

        parseResult = h2i.import_parse(bucket=bucket, path=csvPathname, schema='put', hex_key=hexKey)
        kwargs = {'destination_key': 'rf_model', 'ntrees': trees}
        rfResult = h2o_cmd.runRF(parseResult=parseResult, timeoutSecs=timeoutSecs, **kwargs)
        (classification_error, classErrorPctList, totalScores) = h2o_rf.simpleCheckRFView(rfv=rfResult)


        print "Use H2O GeneratePredictionsPage with a H2O generated model and the same data key."
        print "Does this work? (feeding in same data key)if you're predicting, "
        print "don't you need one less column (the last is output?)"
        print "WARNING: max_iter set to 8 for benchmark comparisons"
        predict_and_compare_csvs(model_key='rf_model', translate=translate)


if __name__ == '__main__':
    h2o.unit_main()
