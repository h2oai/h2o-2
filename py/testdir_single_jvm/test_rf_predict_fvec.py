import unittest, time, sys
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_import as h2i, h2o_browse as h2b

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init(1)

    @classmethod
    def tearDownClass(cls):
        # h2o.sleep(3600)
        h2o.tear_down_cloud()

    def test_rf_predict_fvec(self):
        h2b.browseTheCloud()
        SYNDATASETS_DIR = h2o.make_syn_dir()

        trees = 6
        timeoutSecs = 20
        hex_key = 'iris2.csv.hex'
        parseResult = h2i.import_parse(bucket='smalldata', path='iris/iris2.csv', schema='put', hex_key=hex_key)
        h2o_cmd.runRF(parseResult=parseResult, ntrees=trees, destination_key="iris_rf_model", timeoutSecs=timeoutSecs)

        print "Use H2O GeneratePredictionsPage with a H2O generated model and the same data key. Inspect/Summary result"

        start = time.time()
        predict = h2o.nodes[0].generate_predictions(model_key="iris_rf_model", data_key=hex_key, 
            prediction='predict.hex')
        print "generate_predictions end on ", hex_key, " took", time.time() - start, 'seconds'
        print "predict:", h2o.dump_json(predict)
        csvPredictPathname = SYNDATASETS_DIR + "/" + "iris2.predict.csv"
        h2o.nodes[0].csv_download(src_key='predict.hex', csvPathname=csvPredictPathname)

        inspect = h2o_cmd.runInspect(key='predict.hex')
        print "inspect:", h2o.dump_json(inspect)

        # print h2o.dump_json(predict)
        # no min/max any more with enums?

        expectedCols = {
              # "max": 2.0, 
              # "mean": 1.0, 
              # "min": 0.0, 
              "naCnt": 0, 
              # "name": 0, 
              # Enum or real?
              # "type": "Real", 
        }

        predictCols = inspect['cols'][0]
        diffKeys = [k for k in expectedCols if predictCols[k] != expectedCols[k]]
        for k in diffKeys:
            raise Exception ("Checking H2O summary results, wrong %s: %s, should be: %s" % (k, predictCols[k], expectedCols[k]))

        expected = {
          "numRows": 150, 
          "numCols": 4, 
          # "byteSize": 2843, 
        }

        diffKeys = [k for k in expected if inspect[k] != expected[k]]
        print "diffKeys", diffKeys
        for k in diffKeys:
            raise Exception ("%s : %s != %s" % (k, inspect[k], expected[k]))

if __name__ == '__main__':
    h2o.unit_main()
