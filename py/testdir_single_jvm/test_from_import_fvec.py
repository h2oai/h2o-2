import unittest, time, sys
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i, h2o_rf
import time, random

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init(java_heap_GB=14, enable_benchmark_log=True)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_from_import_fvec(self):
        csvFilenameAll = [
            ("covtype.data", 500),
            # ("covtype20x.data", 1000),
            ]

        for (csvFilename, timeoutSecs) in csvFilenameAll:
            # creates csvFilename.hex from file in importFolder dir 
            hex_key = csvFilename + '.hex'
            parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path="standard/" + csvFilename, schema='local',
                hex_key=hex_key, timeoutSecs=timeoutSecs, doSummary=False)
            print "Parse result['destination_key']:", parseResult['destination_key']

            inspect = h2o_cmd.runInspect(key=parseResult['destination_key'], verbose=True)
            h2o_cmd.infoFromInspect(inspect, parseResult['destination_key'])

            summaryResult = h2o_cmd.runSummary(key=parseResult['destination_key'])
            # h2o_cmd.infoFromSummary(summaryResult)

            trees = 2
            start = time.time()
            rfView = h2o_cmd.runRF(trees=trees, max_depth=20, balance_classes=0, importance=1, parseResult=parseResult, timeoutSecs=timeoutSecs)
            elapsed = time.time() - start

            (classification_error, classErrorPctList, totalScores) = h2o_rf.simpleCheckRFView(rfv=rfView, ntree=trees)

            l = '{!s} jvms, {!s}GB heap, {:s} {:s} {:.2f} secs. \
                trees: {:} classification_error: {:} classErrorPct: {:} totalScores: {:}' .format(
                len(h2o.nodes), h2o.nodes[0].java_heap_GB, 'DRF2', csvFilename, elapsed, 
                    trees, classification_error, classErrorPctList, totalScores)
            print "\n"+l
            h2o.cloudPerfH2O.message(l)

            # just to make sure we test this
            h2i.delete_keys_at_all_nodes(pattern=hex_key)

if __name__ == '__main__':
    h2o.unit_main()
