import unittest, random, sys, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i, h2o_gbm, h2o_jobs as h2j, h2o_import


DO_PLOT = True
print "This will also test set_column_names into GBM, with changing col names per test"

def write_syn_dataset(csvPathname, rowCount, colCount, SEED):
    r1 = random.Random(SEED)
    dsf = open(csvPathname, "w+")

    for i in range(rowCount):
        rowData = []
        for j in range(colCount):
            ri = r1.randint(0,1)
            rowData.append(ri)

        ri = r1.randint(0,1)
        rowData.append(ri)

        rowDataCsv = ",".join(map(str,rowData))
        dsf.write(rowDataCsv + "\n")

    dsf.close()

def write_syn_header(hdrPathname, rowCount, colCount, prefix):
    dsf = open(hdrPathname, "w+")
    rowData = [prefix + "_" + str(j) for j in range(colCount)]
    # add 1 for the output
    rowData.append(prefix + '_response')
    dsf.write(",".join(rowData) + "\n")
    dsf.close()

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init(1,java_heap_GB=10, enable_benchmark_log=True)

    @classmethod
    def tearDownClass(cls):
        ### time.sleep(3600)
        h2o.tear_down_cloud()

    def test_GBM_many_cols(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()

        if h2o.localhost:
            tryList = [
                (10000, 100, 'cA', 300), 
                ]
        else:
            tryList = [
                # (10000, 10, 'cB', 300), 
                # (10000, 50, 'cC', 300), 
                (10000, 100, 'cD', 300), 
                (10000, 200, 'cE', 300), 
                (10000, 300, 'cF', 300), 
                (10000, 400, 'cG', 300), 
                (10000, 500, 'cH', 300), 
                (10000, 1000, 'cI', 300), 
                ]

        for (rowCount, colCount, hex_key, timeoutSecs) in tryList:
            SEEDPERFILE = random.randint(0, sys.maxint)
            # csvFilename = 'syn_' + str(SEEDPERFILE) + "_" + str(rowCount) + 'x' + str(colCount) + '.csv'
            csvFilename = 'syn_' + "binary" + "_" + str(rowCount) + 'x' + str(colCount) + '.csv'
            hdrFilename = 'hdr_' + "binary" + "_" + str(rowCount) + 'x' + str(colCount) + '.csv'

            csvPathname = SYNDATASETS_DIR + '/' + csvFilename
            hdrPathname = SYNDATASETS_DIR + '/' + hdrFilename
            print "Creating random", csvPathname
            write_syn_dataset(csvPathname, rowCount, colCount, SEEDPERFILE)


            # PARSE train****************************************
            start = time.time()
            xList = []
            eList = []
            fList = []

            modelKey = 'GBMModelKey'

            # Parse (train)****************************************
            parseTrainResult = h2i.import_parse(bucket=None, path=csvPathname, schema='put',
                hex_key=hex_key, timeoutSecs=timeoutSecs, 
                doSummary=False)
            # hack

            elapsed = time.time() - start
            print "train parse end on ", csvPathname, 'took', elapsed, 'seconds',\
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

            # GBM(train iterate)****************************************
            ntrees = 5 
            prefixList = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H']
            # for max_depth in [5,10,20,40]:
            for max_depth in [5, 10, 20]:

                # PARSE a new header****************************************
                print "Creating new header", hdrPathname
                prefix = prefixList.pop(0)
                write_syn_header(hdrPathname, rowCount, colCount, prefix)

                # upload and parse the header to a hex

                hdr_hex_key = prefix + "_hdr.hex"
                parseHdrResult = h2i.import_parse(bucket=None, path=hdrPathname, schema='put',
                    header=1, # REQUIRED! otherwise will interpret as enums
                    hex_key=hdr_hex_key, timeoutSecs=timeoutSecs, doSummary=False)
                # Set Column Names (before autoframe is created)
                h2o.nodes[0].set_column_names(source=hex_key, copy_from=hdr_hex_key)

                # GBM
                print "response col name is changing each iteration: parsing a new header"
                params = {
                    'learn_rate': .2,
                    'nbins': 1024,
                    'ntrees': ntrees,
                    'max_depth': max_depth,
                    'min_rows': 10,
                    'response': prefix + "_response",
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
                    len(h2o.nodes), h2o.nodes[0].java_heap_GB, algo, csvFilename, trainElapsed)
                print l
                h2o.cloudPerfH2O.message(l)

                gbmTrainView = h2o_cmd.runGBMView(model_key=modelKey)
                # errrs from end of list? is that the last tree?
                errsLast = gbmTrainView['gbm_model']['errs'][-1]
                print "GBM 'errsLast'", errsLast

                cm = gbmTrainView['gbm_model']['cms'][-1]['_arr'] # use the last one
                pctWrongTrain = h2o_gbm.pp_cm_summary(cm);
                print "\nTrain\n==========\n"
                print h2o_gbm.pp_cm(cm)

                # xList.append(ntrees)
                xList.append(max_depth)
                eList.append(pctWrongTrain)
                fList.append(trainElapsed)

                # works if you delete the autoframe
                ### h2o_import.delete_keys_at_all_nodes(pattern='autoframe')

        # just plot the last one
        if DO_PLOT:
            xLabel = 'max_depth'
            eLabel = 'pctWrong'
            fLabel = 'trainElapsed'
            eListTitle = ""
            fListTitle = ""
            h2o_gbm.plotLists(xList, xLabel, eListTitle, eList, eLabel, fListTitle, fList, fLabel)

if __name__ == '__main__':
    h2o.unit_main()
