import unittest, random, sys, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_import as h2i, h2o_jobs, h2o_gbm

DO_PLOT = True


class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init(1, java_heap_GB=12)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_speedrf_covtype_fvec(self):
        importFolderPath = "standard"

        # Parse Train ******************************************************
        # csvTrainFilename = 'covtype.data'
        csvTrainFilename = 'covtype20x.data'
        csvTrainPathname = importFolderPath + "/" + csvTrainFilename
        hex_key = csvTrainFilename + ".hex"
        parseTrainResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvTrainPathname, hex_key=hex_key,
            timeoutSecs=180, doSummary=False)
        inspect = h2o_cmd.runInspect(None, parseTrainResult['destination_key'])

        xList = []
        eList = []
        fList = []
        trial = 0
        for trial in range(10):
            timeoutSecs = 30
            # have unique model names
            start = time.time()
            summaryResult = h2o_cmd.runSummary(key=hex_key, timeoutSecs=timeoutSecs)
            elapsed = time.time() - start
            print 'summary end', trial, 'on', csvTrainPathname, 'took', elapsed, 'seconds'

            fList.append(elapsed)
            eList.append(elapsed)

            if DO_PLOT:
                xLabel = 'trial'
                xList.append(trial)

        if DO_PLOT:
            eLabel = 'elapsed'
            fLabel = 'elapsed'
            eListTitle = ""
            fListTitle = ""
            h2o_gbm.plotLists(xList, xLabel, eListTitle, eList, eLabel, fListTitle, fList, fLabel)

if __name__ == '__main__':
    h2o.unit_main()
