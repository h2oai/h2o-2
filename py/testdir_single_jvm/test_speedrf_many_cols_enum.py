import unittest, random, sys, time, getpass
sys.path.extend(['.','..','../..','py'])

# FIX! add cases with shuffled data!
import h2o, h2o_cmd, h2o_rf, h2o_gbm
import h2o_browse as h2b, h2o_import as h2i, h2o_exec as h2e, h2o_jobs as h2j


def write_syn_dataset(csvPathname, rowCount, colCount, SEED, translateList):
    # do we need more than one random generator?
    r1 = random.Random(SEED)
    dsf = open(csvPathname, "w+")

    for i in range(rowCount):
        rowData = []
        for j in range(colCount):
            ### ri1 = int(r1.triangular(0,2,1.5))
            ri1 = int(r1.triangular(1,5,2.5))
            rowData.append(ri1)

        rowTotal = sum(rowData)
        ### print rowData
        if translateList is not None:
            for i, iNum in enumerate(rowData):
                # numbers should be 1-5, mapping to a-d
                rowData[i] = translateList[iNum-1]

        rowAvg = (rowTotal + 0.0)/colCount
        ### print rowAvg
        if rowAvg > 2.25:
            result = 1
        else:
            result = 0
        ### print colCount, rowTotal, result
        rowDataStr = map(str,rowData)
        rowDataStr.append(str(result))
        rowDataCsv = ",".join(rowDataStr)
        dsf.write(rowDataCsv + "\n")

    dsf.close()


class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED, tryHeap
        tryHeap = 12
        SEED = h2o.setup_random_seed()
        h2o.init(1, enable_benchmark_log=True, java_heap_GB=tryHeap)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_speedrf_many_cols_enum(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        translateList = ['a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u']

        tryList = [
            (10000, 100, 'cA', 300),
            (10000, 300, 'cB', 500),
            # (10000,  500, 'cC', 700),
            # (10000,  700, 'cD', 3600),
            # (10000,  900, 'cE', 3600),
            # (10000,  1000, 'cF', 3600),
            # (10000,  1300, 'cG', 3600),
            # (10000,  1700, 'cH', 3600),
            # (10000,  2000, 'cI', 3600),
            # (10000,  2500, 'cJ', 3600),
            (10000, 3000, 'cK', 3600),
        ]

        ### h2b.browseTheCloud()
        for (rowCount, colCount, hex_key, timeoutSecs) in tryList:
            SEEDPERFILE = random.randint(0, sys.maxint)
            # csvFilename = 'syn_' + str(SEEDPERFILE) + "_" + str(rowCount) + 'x' + str(colCount) + '.csv'
            csvFilename = 'syn_' + "binary" + "_" + str(rowCount) + 'x' + str(colCount) + '.csv'
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename
            print "Creating random", csvPathname
            write_syn_dataset(csvPathname, rowCount, colCount, SEEDPERFILE, translateList)

            # PARSE train****************************************
            start = time.time()
            xList = []
            eList = []
            fList = []

            modelKey = 'RFModelKey'

            # Parse (train)****************************************
            start = time.time()
            parseTrainResult = h2i.import_parse(bucket=None, path=csvPathname, schema='put', header=0,
                                                hex_key=hex_key, timeoutSecs=timeoutSecs, doSummary=False)
            elapsed = time.time() - start
            print "train parse end on ", csvPathname, 'took', elapsed, 'seconds', \
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
            print "train parse result:", parseTrainResult['destination_key']

            # Logging to a benchmark file
            algo = "Parse"
            l = '{:d} jvms, {:d}GB heap, {:s} {:s} {:6.2f} secs'.format(
                len(h2o.nodes), h2o.nodes[0].java_heap_GB, algo, csvFilename, elapsed)
            print l
            h2o.cloudPerfH2O.message(l)

            inspect = h2o_cmd.runInspect(key=parseTrainResult['destination_key'])
            print "\n" + csvPathname, \
                "    numRows:", "{:,}".format(inspect['numRows']), \
                "    numCols:", "{:,}".format(inspect['numCols'])
            numRows = inspect['numRows']
            numCols = inspect['numCols']
            ### h2o_cmd.runSummary(key=parsTraineResult['destination_key'])

            # RF(train iterate)****************************************
            ntrees = 10
            for max_depth in [5,10,20,40]:
                params = {
                    'nbins': 1024,
                    'classification': 1,
                    'ntrees': ntrees,
                    'max_depth': max_depth,
                    'response': 'C' + str(numCols-1),
                    'ignored_cols_by_name': None,
                    }

                print "Using these parameters for RF: ", params
                kwargs = params.copy()

                trainStart = time.time()
                rfResult = h2o_cmd.runSpeeDRF(parseResult=parseTrainResult,
                                         timeoutSecs=timeoutSecs, destination_key=modelKey, **kwargs)
                trainElapsed = time.time() - trainStart
                print "RF training completed in", trainElapsed, "seconds. On dataset: ", csvPathname

                # Logging to a benchmark file
                algo = "RF " + " ntrees=" + str(ntrees) + " max_depth=" + str(max_depth)
                l = '{:d} jvms, {:d}GB heap, {:s} {:s} {:6.2f} secs'.format(
                    len(h2o.nodes), h2o.nodes[0].java_heap_GB, algo, csvFilename, trainElapsed)
                print l
                h2o.cloudPerfH2O.message(l)
                rfResult["drf_model"] = rfResult.pop("speedrf_model")
                errsLast = rfResult['drf_model']['errs'][-1]
                print "RF 'errsLast'", errsLast

                cm = rfResult['drf_model']['cms'][-1]['_arr'] # use the last one
                pctWrongTrain = h2o_gbm.pp_cm_summary(cm);
                print "\nTrain\n==========\n"
                print h2o_gbm.pp_cm(cm)

                # xList.append(ntrees)
                xList.append(max_depth)
                eList.append(pctWrongTrain)
                fList.append(trainElapsed)

        # just plot the last one
        if 1==1:
            xLabel = 'max_depth'
            eLabel = 'pctWrong'
            fLabel = 'trainElapsed'
            eListTitle = ""
            fListTitle = ""
            h2o_gbm.plotLists(xList, xLabel, eListTitle, eList, eLabel, fListTitle, fList, fLabel)

if __name__ == '__main__':
    h2o.unit_main()
