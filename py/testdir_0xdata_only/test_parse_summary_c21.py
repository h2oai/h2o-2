import unittest, time, sys, random, string
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_nn, h2o_cmd, h2o_import as h2i

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init(java_heap_GB=12)
            # requires user 0xcustomer to access c21 data. So needs to be run with a -cj *json, 
            # or as user 0xcustomer
            # or a config json for the user needs to exist in this directory (like pytest_config-jenkins.json)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_parse_summary_c21(self):
        importFolderPath = '/mnt/0xcustomer-datasets/c21'
        timeoutSecs = 300

        csvPathname_train = importFolderPath + '/persona_clean_deep.tsv.zip'
        hex_key = 'train.hex'
        parseResult  = h2i.import_parse(path=csvPathname_train, hex_key=hex_key, timeoutSecs=timeoutSecs)

        inspect = h2o_cmd.runInspect(key=hex_key)
        missingValuesList = h2o_cmd.infoFromInspect(inspect, csvPathname_train)
        # self.assertEqual(missingValuesList, [], "%s should have 3 cols of NAs: %s" % (csvPathname_train, missingValuesList)
        numCols = inspect['numCols']
        numRows = inspect['numRows']
        rSummary = h2o_cmd.runSummary(key=hex_key)
        h2o_cmd.infoFromSummary(rSummary, rows=numRows, cols=numCols)

        csvPathname_test  = importFolderPath + '/persona_clean_deep.tsv.zip'
        validation_key = 'test.hex'
        parseResult = h2i.import_parse(path=csvPathname_test, hex_key=validation_key, timeoutSecs=timeoutSecs)

        inspect = h2o_cmd.runInspect(key=hex_key)
        missingValuesList = h2o_cmd.infoFromInspect(inspect, csvPathname_test)
        # self.assertEqual(missingValuesList, [], "%s should have 3 cols of NAs: %s" % (csvPathname_test, missingValuesList)

        numCols = inspect['numCols']
        numRows = inspect['numRows']
        rSummary = h2o_cmd.runSummary(key=hex_key, rows=numRows, cols=numCols)
        h2o_cmd.infoFromSummary(rSummary)

if __name__ == '__main__':
    h2o.unit_main()
