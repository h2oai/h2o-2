import unittest, random, sys, time, getpass
sys.path.extend(['.','..','../..','py'])

# FIX! add cases with shuffled data!
import h2o, h2o_cmd, h2o_gbm
import h2o_browse as h2b, h2o_import as h2i, h2o_exec as h2e, h2o_jobs as h2j


DO_PLOT_IF_KEVIN = False

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED, tryHeap
        tryHeap = 28
        SEED = h2o.setup_random_seed()
        h2o.init(1, enable_benchmark_log=True, java_heap_GB=tryHeap)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GBM_poker_1m(self):
        for trial in range(2):
            # PARSE train****************************************
            start = time.time()
            xList = []
            eList = []
            fList = []

            modelKey = 'GBMModelKey'
            timeoutSecs = 900
            # Parse (train)****************************************
            csvPathname = 'poker/poker-hand-testing.data'
            hex_key = 'poker-hand-testing.data.hex'
            parseTrainResult = h2i.import_parse(bucket='smalldata', path=csvPathname, schema='put',
                hex_key=hex_key, timeoutSecs=timeoutSecs, doSummary=False)
            elapsed = time.time() - start
            print "train parse end on ", csvPathname, 'took', elapsed, 'seconds',\
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
            print "train parse result:", parseTrainResult['destination_key']

            # Logging to a benchmark file
            algo = "Parse"
            l = '{:d} jvms, {:d}GB heap, {:s} {:s} {:6.2f} secs'.format(
                len(h2o.nodes), h2o.nodes[0].java_heap_GB, algo, csvPathname, elapsed)
            print l
            h2o.cloudPerfH2O.message(l)

            inspect = h2o_cmd.runInspect(key=parseTrainResult['destination_key'])
            print "\n" + csvPathname, \
                "    numRows:", "{:,}".format(inspect['numRows']), \
                "    numCols:", "{:,}".format(inspect['numCols'])
            numRows = inspect['numRows']
            numCols = inspect['numCols']
            ### h2o_cmd.runSummary(key=parsTraineResult['destination_key'])

            # GBM(train iterate)****************************************
            ntrees = 2
            for max_depth in [5,10,20]:
                params = {
                    'learn_rate': .1,
                    'nbins': 10,
                    'ntrees': ntrees,
                    'max_depth': max_depth,
                    'min_rows': 10,
                    'response': numCols-1,
                    'ignored_cols_by_name': None,
                }
                print "Using these parameters for GBM: ", params
                kwargs = params.copy()

                trainStart = time.time()
                gbmTrainResult = h2o_cmd.runGBM(parseResult=parseTrainResult,
                    timeoutSecs=timeoutSecs, destination_key=modelKey, **kwargs)
                trainElapsed = time.time() - trainStart
                print "GBM training completed in", trainElapsed, "seconds. On dataset: ", csvPathname

                # Logging to a benchmark file
                algo = "GBM " + " ntrees=" + str(ntrees) + " max_depth=" + str(max_depth)
                l = '{:d} jvms, {:d}GB heap, {:s} {:s} {:6.2f} secs'.format(
                    len(h2o.nodes), h2o.nodes[0].java_heap_GB, algo, csvPathname, trainElapsed)
                print l
                h2o.cloudPerfH2O.message(l)

                gbmTrainView = h2o_cmd.runGBMView(model_key=modelKey)
                # errrs from end of list? is that the last tree?
                errsLast = gbmTrainView['gbm_model']['errs'][-1]
                print "GBM 'errsLast'", errsLast

                cm = gbmTrainView['gbm_model']['cms'][-1]['_arr'] # use the last one
                pctWrongTrain = h2o_gbm.pp_cm_summary(cm);
                print "Last line of this cm might be NAs, not CM"
                print "\nTrain\n==========\n"
                print h2o_gbm.pp_cm(cm)

                # xList.append(ntrees)
                xList.append(max_depth)
                eList.append(pctWrongTrain)
                fList.append(trainElapsed)

        # just plot the last one
        if DO_PLOT_IF_KEVIN:
            xLabel = 'max_depth'
            eLabel = 'pctWrong'
            fLabel = 'trainElapsed'
            eListTitle = ""
            fListTitle = ""
            h2o_gbm.plotLists(xList, xLabel, eListTitle, eList, eLabel, fListTitle, fList, fLabel)

if __name__ == '__main__':
    h2o.unit_main()
