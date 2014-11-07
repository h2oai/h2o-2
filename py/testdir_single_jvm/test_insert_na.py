import unittest, time, sys, random 
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_glm, h2o_import as h2i, h2o_jobs, h2o_exec as h2e, h2o_util

DO_POLL = False
class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()

        h2o.init(java_heap_GB=4)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_insert_na(self):
        csvFilename = 'covtype.data'
        csvPathname = 'standard/' + csvFilename
        hex_key = "covtype.hex"

        parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, hex_key=hex_key, schema='local', timeoutSecs=20)

        print "Just insert some NAs and see what happens"
        inspect = h2o_cmd.runInspect(key=hex_key)
        origNumRows = inspect['numRows']
        origNumCols = inspect['numCols']
        missing_fraction = 0.1

        # every iteration, we add 0.1 more from the unmarked to the marked (missing)

        expectedMissing = missing_fraction * origNumRows # per col
        for trial in range(2):

            fs = h2o.nodes[0].insert_missing_values(key=hex_key, missing_fraction=missing_fraction, seed=SEED)
            print "fs", h2o.dump_json(fs)
            inspect = h2o_cmd.runInspect(key=hex_key)
            numRows = inspect['numRows']
            numCols = inspect['numCols']
            expected = .1 * numRows

            # Each column should get .10 random NAs per iteration. Within 10%? 
            missingValuesList = h2o_cmd.infoFromInspect(inspect)
            print "missingValuesList", missingValuesList
            for mv in missingValuesList:
                # h2o_util.assertApproxEqual(mv, expectedMissing, tol=0.01, msg='mv %s is not approx. expected %s' % (mv, expectedMissing))
                self.assertAlmostEqual(mv, expectedMissing, delta=0.1 * mv, msg='mv %s is not approx. expected %s' % (mv, expectedMissing))

            self.assertEqual(origNumRows, numRows)
            self.assertEqual(origNumCols, numCols)

            summaryResult = h2o_cmd.runSummary(key=hex_key)
            # h2o_cmd.infoFromSummary(summaryResult)

            print "trial", trial
            print "expectedMissing:", expectedMissing
            print "I don't understand why the values don't increase every iteration. It seems to stay stuck with the first effect"

if __name__ == '__main__':
    h2o.unit_main()
