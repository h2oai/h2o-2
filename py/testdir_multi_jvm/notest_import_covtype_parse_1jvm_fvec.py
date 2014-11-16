import unittest, sys, random, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_browse as h2b, h2o_import as h2i

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        pass
        print "Will build clouds with incrementing heap sizes and import folder/parse"

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_import_covtype_parse_1jvm_fvec(self):
        csvFilename = "covtype.data"
        importFolderPath = "standard"
        trialMax = 2
        for tryHeap in [4,3,2,1]:
            print "\n", tryHeap,"GB heap, 1 jvms, import folder, then loop parsing 'covtype.data' to unique keys"
            h2o.init(java_heap_GB=tryHeap)

            for trial in range(trialMax):
                # import each time, because h2o deletes source file after parse
                csvPathname = importFolderPath + "/" + csvFilename
                hex_key = csvFilename + "_" + str(trial) + ".hex"
                parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, hex_key=hex_key, timeoutSecs=20)
            # sticky ports?
            h2o.tear_down_cloud()
            time.sleep(2)

if __name__ == '__main__':
    h2o.unit_main()
