import unittest, time, sys
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_import as h2i, h2o_rf

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init(3, java_heap_GB=4)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_rf_parity_cmp(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()

        # always match the run below!
        # just using one file for now
        for x in [50000]:
            shCmdString = "perl " + h2o.find_file("syn_scripts/parity.pl") + " 128 4 "+ str(x) + " quad " + SYNDATASETS_DIR
            h2o.spawn_cmd_and_wait('parity.pl', shCmdString.split(),4)
            csvFilename = "parity_128_4_" + str(x) + "_quad.data"  

        def doBoth():
            h2o.verboseprint("Trial", trial)
            start = time.time()
            # make sure ntrees and max_depth are the same for both
            rfView = h2o_cmd.runRF(parseResult=parseResult, ntrees=ntrees, max_depth=40, response=response,
                timeoutSecs=600, retryDelaySecs=3)
            elapsed1 = time.time() - start
            (totalError1, classErrorPctList1, totalScores2) = h2o_rf.simpleCheckRFView(rfv=rfView)

            rfView = h2o_cmd.runSpeeDRF(parseResult=parseResult, ntrees=ntrees, max_depth=40, response=response,
                timeoutSecs=600, retryDelaySecs=3)
            elapsed2 = time.time() - start
            (totalError2, classErrorPctList2, totalScores2) = h2o_rf.simpleCheckRFView(rfv=rfView)

            print "Checking that results are similar (within 20%)"
            print "DRF2 then SpeeDRF"
            print "per-class variance is large..basically we can't check very well for this dataset"
            for i, (j,k) in enumerate(zip(classErrorPctList1, classErrorPctList2)):
                print "classErrorPctList[%s]:i %s %s" % (i, j, k)
                # self.assertAlmostEqual(classErrorPctList1[i], classErrorPctList2[i], 
                #    delta=1 * classErrorPctList2[i], msg="Comparing RF class %s errors for DRF2 and SpeeDRF" % i)

            print "totalError: %s %s" % (totalError1, totalError2)
            self.assertAlmostEqual(totalError1, totalError2, delta=.2 * totalError2, msg="Comparing RF total error for DRF2 and SpeeDRF")
            print "elapsed: %s %s" % (elapsed1, elapsed2)
            self.assertAlmostEqual(elapsed1, elapsed2, delta=.5 * elapsed2, msg="Comparing RF times for DRF2 and SpeeDRF")

        # always match the gen above!
        for trial in range (1):
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename

            hex_key = csvFilename + "_" + str(trial) + ".hex"
            parseResult = h2o_cmd.parseResult = h2i.import_parse(path=csvPathname, schema='put', hex_key=hex_key, timeoutSecs=30, doSummary=False)

            inspect = h2o_cmd.runInspect(key=hex_key)
            numCols = inspect['numCols']
            numRows = inspect['numRows']
            response = "C" + str(numCols)
            ntrees = 30

            doBoth()
            print "*****************************"
            print "end # %s RF compare" % trial, 
            print "*****************************"

            print "Now change all cols to enums"
            for e in range(numCols):
                enumResult = h2o.nodes[0].to_enum(src_key=hex_key, column_index=(e+1))


            doBoth()
            print "*********************************"
            print "end # %s RF compare, with enums #" % trial, 
            print "*********************************"

if __name__ == '__main__':
    h2o.unit_main()
