import unittest, time, sys, random
sys.path.extend(['.','..','py'])
import h2o, h2o_hosts, h2o_browse as h2b, h2o_import2 as h2i, 

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(1,java_heap_GB=10)
        else:
            h2o_hosts.build_cloud_with_hosts(1,java_heap_GB=10)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_from_import(self):
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
                # creates csvFilename.hex from file in importFolder dir 
                start = time.time()
                parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvFilename, timeoutSecs=500)
                elapsed = time.time() - start
                print csvFilename, "parsed in", elapsed, "seconds.", "%d pct. of timeout" % ((elapsed*100)/timeoutSecs), "\n"
                print csvFilename, 'H2O reports parse time:', parseResult['response']['time']

                # h2o doesn't produce this, but h2o_import.py adds it for us.
                print "Parse result['python_source_key']:", parseResult['python_source_key']
                print "Parse result['destination_key']:", parseResult['destination_key']
                print "\n" + csvFilename

                storeView = h2o.nodes[0].store_view()
                print "storeView:", h2o.dump_json(storeView)
                # h2o deletes key after parse now
                ## print "Removing", parseResult['python_source_key'], "so we can re-import it"
                ## removeKeyResult = h2o.nodes[0].remove_key(key=parseResult['python_source_key'])
                ## print "removeKeyResult:", h2o.dump_json(removeKeyResult)

            print "\nTrial", trial, "completed\n"

if __name__ == '__main__':
    h2o.unit_main()
