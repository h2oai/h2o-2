import unittest, time, sys, random, string

sys.path.extend(['.','..','../..','py'])
import h2o, h2o_gbm, h2o_cmd, h2o_import as h2i, h2o_browse as h2b

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init(java_heap_GB=2)

    @classmethod
    def tearDownClass(cls):
        ###h2o.sleep(3600)
        h2o.tear_down_cloud()

    def test_NN_airlines_small(self):
        #h2b.browseTheCloud()
        csvPathname_train = 'airlines/AirlinesTrain.csv.zip'
        csvPathname_test  = 'airlines/AirlinesTest.csv.zip'
        hex_key = 'airlines_train.hex'
        validation_key = 'airlines_test.hex'
        timeoutSecs = 30
        parseResult  = h2i.import_parse(bucket='smalldata', path=csvPathname_train, schema='put', hex_key=hex_key, timeoutSecs=timeoutSecs)
        parseResultV = h2i.import_parse(bucket='smalldata', path=csvPathname_test, schema='put', hex_key=validation_key, timeoutSecs=timeoutSecs)

        inspect = h2o_cmd.runInspect(None, hex_key)
        print "\n" + csvPathname_train, \
            "    numRows:", "{:,}".format(inspect['numRows']), \
            "    numCols:", "{:,}".format(inspect['numCols'])
        # this gives the last col number, which is IsDepDelayed_REC (1 or -1)
        # response = inspect['numCols'] - 1

        # this is "YES"/"NO"
        response = 'IsDepDelayed'

        #Making random id
        identifier = ''.join(random.sample(string.ascii_lowercase + string.digits, 10))
        model_key = 'nn_' + identifier + '.hex'

        # get the column names
        colNames = [c['name'] for c in inspect['cols']]
        print "colNames:", colNames
        usedCols = ("Origin", "Dest", "fDayofMonth", "fYear", "UniqueCarrier", "fDayOfWeek", "fMonth", "DepTime", "ArrTime", "Distance")

        ignoredCols = []
        for c in colNames:
            # don't put the response in the ignore list (is there a problem if so?)
            if c not in usedCols and c != response:
                ignoredCols.append(c)

        ignoredColsString = ",".join(ignoredCols)
        print "Telling h2o to ignore these cols:"
        print ignoredColsString

        kwargs = {
            'ignored_cols'                 : ignoredColsString,
            'response'                     : response,
            'classification'               : 1,
            'destination_key'              : model_key,
            }
        expectedErr = 0.45 ## expected validation error for the above model
        relTol = 0.50 ## 20% rel. error tolerance due to Hogwild!

        timeoutSecs = 600
        start = time.time()
        nn = h2o_cmd.runDeepLearning(parseResult=parseResult, timeoutSecs=timeoutSecs, **kwargs)
        print "neural net end on ", csvPathname_train, " and ", csvPathname_test, 'took', time.time() - start, 'seconds'

        predict_key = 'score_' + identifier + '.hex'

        kwargs = {
            'data_key': validation_key,
            'destination_key': predict_key,
            'model_key': model_key
            }

        predictResult = h2o_cmd.runPredict(timeoutSecs=timeoutSecs, **kwargs)
        h2o_cmd.runInspect(key=predict_key, verbose=True)

        kwargs = {
        }

        predictCMResult = h2o.nodes[0].predict_confusion_matrix(
            actual=validation_key,
            vactual=response,
            predict=predict_key,
            vpredict='predict',
            timeoutSecs=timeoutSecs, **kwargs)

        cm = predictCMResult['cm']

        print h2o_gbm.pp_cm(cm)
        actualErr = h2o_gbm.pp_cm_summary(cm)/100.;

        print "actual   classification error:" + format(actualErr)
        print "expected classification error:" + format(expectedErr)
        if actualErr != expectedErr and abs((expectedErr - actualErr)/expectedErr) > relTol:
            raise Exception("Scored classification error of %s is not within %s %% relative error of %s" %
                            (actualErr, float(relTol)*100, expectedErr))


if __name__ == '__main__':
    h2o.unit_main()
