import unittest, time, sys, random
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init(1,java_heap_GB=10)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_putfile_a5m(self):
        timeoutSecs = 500
        csvFilenameList = [
            # use different names for each parse 
            # doesn't fail if gzipped?
            ("a5m.csv", 'A', None),
            ("a5m.csv", 'B', None),
            ("a5m.csv", 'C', None),
            ]
        # pop open a browser on the cloud
        h2b.browseTheCloud()

        for (csvFilename, key, trees) in csvFilenameList:
            csvPathname = csvFilename

            # creates csvFilename and csvFilename.hex  keys
            parseResult = h2i.import_parse(path=csvPathname, schema='put', timeoutSecs=500)
            print "Parse result['destination_key']:", parseResult['destination_key']
            inspect = h2o_cmd.runInspect(key=parseResult['destination_key'])

            print "\n" + csvFilename
            start = time.time()
            # constrain depth to 25
            if trees is not None:
                RFview = h2o_cmd.runRF(trees=trees,depth=25,parseResult=parseResult,
                    timeoutSecs=timeoutSecs)

            h2b.browseJsonHistoryAsUrlLastMatch("RFView")
            # wait in case it recomputes it
            time.sleep(10)

            sys.stdout.write('.')
            sys.stdout.flush() 

if __name__ == '__main__':
    h2o.unit_main()
