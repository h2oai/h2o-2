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

    def test_rf_predict3(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()

        trees = 15
        timeoutSecs = 120
        hexKey = 'covtype.data.hex'
        predictHexKey = 'predict.hex'
        predictCsv = 'predict.csv'
        bucket = 'home-0xdiag-datasets'
        csvPathname = 'standard/covtype.data'

        # for using below in csv reader
        csvFullname = h2i.find_folder_and_filename(bucket, csvPathname, schema='put', returnFullPath=True)
        parseResult = h2i.import_parse(bucket=bucket, path=csvPathname, schema='put', hex_key=hexKey)
        h2o_cmd.runRFOnly(parseResult=parseResult, trees=trees, model_key="covtype_rf_model", timeoutSecs=timeoutSecs)
        print "Use H2O GeneratePredictionsPage with a H2O generated model and the same data key. Inspect/Summary result"
        print "Does this work? (feeding in same data key)if you're predicting, don't you need one less column (the last is output?)"

        start = time.time()
        predict = h2o.nodes[0].generate_predictions(model_key="covtype_rf_model", data_key=hexKey, destination_key=predictHexKey)
        print "generate_predictions end on ", hexKey, " took", time.time() - start, 'seconds'
        h2o.check_sandbox_for_errors()
        inspect = h2o_cmd.runInspect(key=predictHexKey)
        h2o_cmd.infoFromInspect(inspect, 'predict.hex')


        csvDownloadPathname = SYNDATASETS_DIR + "/" + predictCsv
        h2o.nodes[0].csv_download(key=predictHexKey, csvPathname=csvDownloadPathname)
        h2o.check_sandbox_for_errors()

        print "Do a check of the original output col against predicted output"
        import csv
        originalOutput = []
        with open(csvFullname, 'rb') as f:
            reader = csv.reader(f)
            print "csv read of", csvPathname
            rowNum1 = 0
            for row in reader:
                # print the last col
                # only print first 10 for seeing
                if rowNum1<10: print "Expected", row[-1]
                # ignore ' response'
                originalOutput.append(row[-1])
                rowNum1 += 1


        predictOutput = []
        with open(csvDownloadPathname, 'rb') as f:
            reader = csv.reader(f)
            print "csv read of", csvDownloadPathname
            rowNum2 = 0
            for row in reader:
                # print the last col
                # ignore the first row ..header
                if rowNum2>0:
                    # only print first 10 for seeing
                    if rowNum2<10: print "Predicted:", row[-1]
                    predictOutput.append(row[-1])
                rowNum2 += 1

        # no header on source
        if ((rowNum1+1) != rowNum2):
            raise Exception("original rowNum1: %s + 1 is not the same as downloaded predict (w/header) rowNum2: %s" % (rowNum1, rowNum2))

        wrong = 0
        for rowNum,(o,p) in enumerate(zip(originalOutput, predictOutput)):
            if float(o)!=float(p): # should be integers 1 to 7?
                msg = "Comparing orignal output col vs predicted. row %s differs. original: %s predicted: %s"  % (rowNum, o, p)
                if wrong == 10:
                    print "Not printing any more mismatches"
                elif wrong < 10:
                    print msg
                wrong += 1

        print "\nTotal wrong:", wrong
        print "Total:", len(originalOutput)
        pctWrong = (100.0 * wrong)/len(originalOutput)
        print "wrong/Total * 100 ", pctWrong
        if pctWrong > 10.0:
            raise Exception("pct wrong too high")

if __name__ == '__main__':
    h2o.unit_main()
