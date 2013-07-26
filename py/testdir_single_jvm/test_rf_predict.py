import os, json, unittest, time, shutil, sys
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts

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
        csvPathname = h2o.find_file('smalldata/iris/iris2.csv')
        h2o_cmd.runRF(trees=trees, model_key="iris_rf_model", timeoutSecs=timeoutSecs, csvPathname=csvPathname)
        print "\Use H2O GeneratePredictionsPage with a H2O generated model and the same data key. Inspect/Summary result"

        start = time.time()
        key2 = "iris2.csv.hex"
        predict = h2o.nodes[0].generate_predictions(model_key="iris_rf_model", key=key2)
        print "generate_predictions end on ",  key2, " took", time.time() - start, 'seconds'

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
