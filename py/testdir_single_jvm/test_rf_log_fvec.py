import unittest, random, sys, time, math
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_rf as h2o_rf, h2o_import as h2i, h2o_exec, h2o_util
import h2o_browse as h2b

print "This test will be good for testing RF against continuous feature (note the output depends only on col 0)"
print "We have to make the output integer though, still (no regression support?"
# we can pass ntree thru kwargs if we don't use the "trees" parameter in runRF
paramDict = {
    # FIX! if there's a header, can you specify column number or column header
    'ntrees': 10,
    'destination_key': 'model_keyA',
    'max_depth': 20, 
    'nbins': 1000,
    ## 'seed': 3,
    ## 'mtries': 30,
    }

def write_syn_dataset(csvPathname, rowCount, colCount, SEED):
    r1 = random.Random(SEED)
    dsf = open(csvPathname, "w+")

    for i in range(rowCount):
        rowData = []
        for j in range(colCount):
            ri = r1.uniform(1,129)
            rowData.append(ri)

        # just use col 0 to determine the output (log(x) + some random noise)
        ## ri = math.log(rowData[0]) + r1.normalvariate(0, 0.3) 
        ## ri = int(round(ri,1) * 10) # rf needs integer response, for now?
        ri = math.log(rowData[0])
        ri = int(round(ri,0)) # rf needs integer response, for now?
        rowData.append(ri)

        rowDataCsv = ",".join(map(str,rowData))
        dsf.write(rowDataCsv + "\n")

    dsf.close()


class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init(java_heap_GB=10)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_rf_log_fvec(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()

        tryList = [
            (10000, 100, 'cA', 300),
            ]

        for (rowCount, colCount, hex_key, timeoutSecs) in tryList:
            SEEDPERFILE = random.randint(0, sys.maxint)

            # CREATE test dataset******************************************************
            csvFilename = 'syn_test_' + str(rowCount) + 'x' + str(colCount) + '.csv'
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename
            print "Creating random", csvPathname
            write_syn_dataset(csvPathname, rowCount, colCount, SEEDPERFILE)
            testParseResult = h2i.import_parse(path=csvPathname, hex_key=hex_key, schema='put', timeoutSecs=10)
            print "Test Parse result['destination_key']:", testParseResult['destination_key']
            dataKeyTest = testParseResult['destination_key']

            # CREATE train dataset******************************************************
            csvFilename = 'syn_train_' + str(rowCount) + 'x' + str(colCount) + '.csv'
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename
            print "Creating random", csvPathname
            write_syn_dataset(csvPathname, rowCount, colCount, SEEDPERFILE)
            trainParseResult = h2i.import_parse(path=csvPathname, hex_key=hex_key, schema='put', timeoutSecs=10)
            print "Train Parse result['destination_key']:", trainParseResult['destination_key']
            dataKeyTrain = trainParseResult['destination_key']


            # RF train******************************************************
            # adjust timeoutSecs with the number of trees
            # seems ec2 can be really slow
            kwargs = paramDict.copy()
            timeoutSecs = 30 + kwargs['ntrees'] * 20
            start = time.time()
            # do oobe
            kwargs['response'] = "C" + str(colCount+1)
            
            rfv = h2o_cmd.runRF(parseResult=trainParseResult, timeoutSecs=timeoutSecs, **kwargs)
            elapsed = time.time() - start
            print "RF end on ", csvPathname, 'took', elapsed, 'seconds.', \
                "%d pct. of timeout" % ((elapsed/timeoutSecs) * 100)

            rf_model = rfv['drf_model']
            used_trees = rf_model['N']
            data_key = rf_model['_dataKey']
            model_key = rf_model['_key']

            (classification_error, classErrorPctList, totalScores) = h2o_rf.simpleCheckRFView(rfv=rfv, ntree=used_trees)
            oobeTrainPctRight = 100.0 - classification_error
            expectTrainPctRight = 94
            h2o_util.assertApproxEqual(oobeTrainPctRight, expectTrainPctRight, rel=.1,
                msg="OOBE: pct. right for training not close enough %6.2f %6.2f" % (oobeTrainPctRight, expectTrainPctRight))

            # RF score******************************************************
            print "Now score with the 2nd random dataset"
            rfv = h2o_cmd.runRFView(data_key=dataKeyTest, model_key=model_key, 
                timeoutSecs=timeoutSecs, retryDelaySecs=1)

            (classification_error, classErrorPctList, totalScores) = h2o_rf.simpleCheckRFView(rfv=rfv, ntree=used_trees)
            h2o_util.assertApproxEqual(classification_error, 8.0, tol=4,
                msg="Classification error %s too big" % classification_error)

            predict = h2o.nodes[0].generate_predictions(model_key=model_key, data_key=dataKeyTest)

            fullScorePctRight = 100.0 - classification_error
            expectScorePctRight = 94
            h2o_util.assertApproxEqual(fullScorePctRight, expectScorePctRight, rel=.1,
                msg="Full: pct. right for scoring not close enough %6.2f %6.2f" % (fullScorePctRight, expectScorePctRight))


if __name__ == '__main__':
    h2o.unit_main()
