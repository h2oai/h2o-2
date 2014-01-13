import unittest, time, sys, random
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_import as h2i
import h2o_browse as h2b

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

    def test_B_putfile_files(self):
        timeoutSecs = 500

        #    "covtype169x.data",
        #    "covtype.13x.shuffle.data",
        #    "3G_poker_shuffle"
        #    "covtype20x.data", 
        #    "billion_rows.csv.gz",
        csvFilenameList = [
            ("covtype.data", 'standard/covtype.data', 1),
            ]
        # pop open a browser on the cloud
        h2b.browseTheCloud()

        for (csvFilename, csvPathname, trees) in csvFilenameList:
            parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, timeoutSecs=500, schema='put')
            print csvFilename, 'parse time:', parseResult['response']['time']
            print "Parse result['destination_key']:", parseResult['destination_key']
            # We should be able to see the parse result?
            inspect2 = h2o_cmd.runInspect(key=parseResult['destination_key'])

            print "\n" + csvFilename
            start = time.time()
            # constrain depth to 25
            if trees is not None:
                RFview = h2o_cmd.runRF(trees=trees,depth=25,parseResult=parseResult,
                    timeoutSecs=timeoutSecs)

            sys.stdout.write('.')
            sys.stdout.flush() 

if __name__ == '__main__':
    h2o.unit_main()
