import unittest, time, sys, random
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_browse as h2b, h2o_import as h2i

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init(1,java_heap_GB=10)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_delete_all_keys(self):
        # FIX! should have some model keys in here too, from RF etc.
        importFolderPath = 'standard'
        timeoutSecs = 500

        csvFilenameAll = [
            "covtype.data",
            "covtype20x.data",
            ]
        # csvFilenameList = random.sample(csvFilenameAll,1)
        csvFilenameList = csvFilenameAll
        for trial in range(3):
            for csvFilename in csvFilenameList:
                csvPathname = importFolderPath + "/" + csvFilename
                start = time.time()
                parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, timeoutSecs=500)
                elapsed = time.time() - start
                print csvFilename, "parsed in", elapsed, "seconds.", "%d pct. of timeout" % ((elapsed*100)/timeoutSecs), "\n"

                print "Parse result['destination_key']:", parseResult['destination_key']
                print "\n" + csvFilename

                print "Delete all keys"
                h2o.nodes[0].remove_all_keys()
                print "This shouldn't see any keys"
                h2i.delete_keys_at_all_nodes()



            print "\nTrial", trial, "completed\n"

if __name__ == '__main__':
    h2o.unit_main()
