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

    def test_rf_predict(self):
        trees = 6
        timeoutSecs = 20
        hex_key = 'iris2.csv.hex'
        parseResult = h2i.import_parse(bucket='smalldata', path='iris/iris2.csv', schema='put', hex_key=hex_key)
        h2o_cmd.runRFOnly(parseResult=parseResult, trees=trees, model_key="iris_rf_model", timeoutSecs=timeoutSecs)
        print "\Use H2O GeneratePredictionsPage with a H2O generated model and the same data key. Inspect/Summary result"

        start = time.time()
        predict = h2o.nodes[0].generate_predictions(model_key="iris_rf_model", data_key=hex_key)
        print "generate_predictions end on ", hex_key, " took", time.time() - start, 'seconds'

        # print h2o.dump_json(predict)
        expectedCols = {
              "base": 0, 
              "enum_domain_size": 0, 
              "max": 2.0, 
              "mean": 1.0, 
              "min": 0.0, 
              "num_missing_values": 0, 
              "offset": 0, 
              "scale": 1, 
              "size": 8, 
              "type": "float", 
              "variance": 0.816496580927726
        }

        predictCols = predict['cols'][0]
        diffkeys = [k for k in expectedCols if predictCols[k] != expectedCols[k]]
        for k in diffkeys:
            raise Exception ("Checking H2O summary results, wrong %s: %s, should be: %s" % (k, predictCols[k], expectedCols[k]))

        expected = {
          "num_rows": 150, 
          "num_cols": 1, 
          "row_size": 8, 
        }

        diffkeys = [k for k in expected if predict[k] != expected[k]]
        for k in diffkeys:
            raise Exception ("%s : %s != %s" % (k, predict[k], expected[k]))

if __name__ == '__main__':
    h2o.unit_main()
