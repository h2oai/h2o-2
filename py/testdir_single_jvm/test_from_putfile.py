import os, json, unittest, time, shutil, sys
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd
import h2o_hosts
import h2o_browse as h2b
import h2o_import as h2i
import time, random

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.build_cloud(1,java_heap_GB=14)

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
            ("covtype.data", 1),
            ("covtype20x.data", 1),
            # ("covtype200x.data", None),
            # ("a5m.csv", None),
            # ("a10m.csv", None),
            # ("a100m.csv", None),
            # ("a200m.csv", None),
            # ("a400m.csv", None),
            # ("a600m.csv", None),
            # ("100million_rows.csv,  None"),
            # ("200million_rows.csv", None),
            # ("billion_rows.csv.gz", 1),
            # memory issue on one machine. no RF
            # ("new-poker-hand.full.311M.txt.gz", None),
            ]
        # pop open a browser on the cloud
        h2b.browseTheCloud()

        for (csvFilename, trees) in csvFilenameList:
            csvPathname = h2o.find_file('/home/0xdiag/datasets/' + csvFilename)

            # creates csvFilename and csvFilename.hex  keys
            parseKey = h2o_cmd.parseFile(csvPathname=csvPathname, key=csvFilename, timeoutSecs=500)
            print csvFilename, 'parse time:', parseKey['response']['time']
            print "Parse result['destination_key']:", parseKey['destination_key']

            # We should be able to see the parse result?
            inspect = h2o_cmd.runInspect(key=parseKey['destination_key'])

            print "\n" + csvFilename
            start = time.time()
            # constrain depth to 25
            if trees is not None:
                RFview = h2o_cmd.runRFOnly(trees=trees,depth=25,parseKey=parseKey,
                    timeoutSecs=timeoutSecs)

            h2b.browseJsonHistoryAsUrlLastMatch("RFView")
            # wait in case it recomputes it
            time.sleep(10)

            sys.stdout.write('.')
            sys.stdout.flush() 

if __name__ == '__main__':
    h2o.unit_main()
