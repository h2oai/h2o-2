import unittest, time, sys
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_import2 as h2i

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

    def test_rf_predict2(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()

        trees = 6
        timeoutSecs = 20
        hexKey = 'iris2.csv.hex'
        predictHexKey = 'predict.hex'
        predictCsv = 'predict.csv'

        # for using below in csv reader
        csvPathname = h2i.find_folder_and_filename('smalldata', 'iris/iris2.csv', schema='put', returnFullPath=True)

        parseResult = h2i.import_parse(bucket='smalldata', path='iris/iris2.csv', schema='put', hex_key=hexKey)
        h2o_cmd.runRFOnly(parseResult=parseResult, trees=trees, model_key="iris_rf_model", timeoutSecs=timeoutSecs)
        print "Use H2O GeneratePredictionsPage with a H2O generated model and the same data key. Inspect/Summary result"

        print "Does this work? (feeding in same data key)if you're predicting, don't you need one less column (the last is output?)"

        start = time.time()
        predict = h2o.nodes[0].generate_predictions(model_key="iris_rf_model", data_key=hexKey, destination_key=predictHexKey)
        print "generate_predictions end on ", hexKey, " took", time.time() - start, 'seconds'
        h2o.check_sandbox_for_errors()
        inspect = h2o_cmd.runInspect(key=predictHexKey)
        h2o_cmd.infoFromInspect(inspect, 'predict.hex')


        csvDownloadPathname = SYNDATASETS_DIR + "/" + predictCsv
        h2o.nodes[0].csv_download(key=predictHexKey, csvPathname=csvDownloadPathname)
        h2o.check_sandbox_for_errors()

        print "Do a check of the original output col against predicted output"
        import csv
        outputTranslate = {'setosa': '0.0', 'versicolor': '1.0', 'virginica': '2.0'}

        originalOutput = []
        with open(csvPathname, 'rb') as f:
            reader = csv.reader(f)
            print "csv read of", csvPathname
            rowNum1 = 0
            for row in reader:
                # print the last col
                # print row[-1]
                # ignore ' response'
                if str(row[-1]) in outputTranslate:
                    originalOutput.append(outputTranslate[str(row[-1])])
                rowNum1 += 1


        predictOutput = []
        with open(csvDownloadPathname, 'rb') as f:
            reader = csv.reader(f)
            print "csv read of", csvDownloadPathname
            rowNum2 = 0
            for row in reader:
                # print the last col
                # ignore the first row ' response'
                if rowNum2>0:
                    predictOutput.append(str(row[-1]))
                rowNum2 += 1

        if (rowNum1 != rowNum2):
            raise Exception("original rowNum1: %s is not the same as downloaded predict rowNum2: %s" % (rowNum1, rowNum2))

        for rowNum,(o,p) in enumerate(zip(originalOutput, predictOutput)):
            if o!=p:
                msg = "Comparing orignal output col vs predicted. row %s differs. original: %s predicted: %s"  % (rowNum, o, p)
                raise Exception(msg)

if __name__ == '__main__':
    h2o.unit_main()
