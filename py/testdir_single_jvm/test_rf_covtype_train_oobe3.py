import unittest
import random, sys, time
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_rf as h2o_rf, h2o_hosts, h2o_import as h2i, h2o_exec
import h2o_browse as h2b

# we can pass ntree thru kwargs if we don't use the "trees" parameter in runRF
# only classes 1-7 in the 55th col
# rng DETERMINISTIC is default
paramDict = {
    # FIX! if there's a header, can you specify column number or column header
    'response_variable': 54,
    'class_weight': None,
    'ntree': 10,
    'model_key': 'model_keyA',
    'out_of_bag_error_estimate': 1,
    'stat_type': 'ENTROPY',
    'depth': 2147483647, 
    'bin_limit': 10000,
    'parallel': 1,
    'ignore': "1,2,6,7,8",
    'sample': 66,
    ## 'seed': 3,
    ## 'features': 30,
    'exclusive_split_limit': 0,
    }

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(node_count=1, java_heap_GB=10)
        else:
            h2o_hosts.build_cloud_with_hosts(node_count=1, java_heap_GB=10)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_rf_covtype_train_oobe2(self):
        print "\nUse randomBitVector and filter to separate the dataset randomly"
        importFolderPath = "/home/0xdiag/datasets/standard"
        csvFilename = 'covtype.data'
        csvPathname = importFolderPath + "/" + csvFilename
        key2 = csvFilename + ".hex"

        h2i.setupImportFolder(None, importFolderPath)
        print "\nUsing header=0 on the normal covtype.data"
        parseKey = h2i.parseImportFolderFile(None, csvFilename, importFolderPath, key2=key2,
            header=0, timeoutSecs=100)

        inspect = h2o_cmd.runInspect(None, parseKey['destination_key'])
        print "\n" + csvPathname, \
            "    num_rows:", "{:,}".format(inspect['num_rows']), \
            "    num_cols:", "{:,}".format(inspect['num_cols'])

        # how many rows for each pct?
        num_rows = inspect['num_rows']
        pct10 = int(num_rows * .1)
        rowsForPct = [i * pct10 for i in range(0,11)]
        # this can be slightly less than 10%
        # last10 = num_rows - rowsForPct[9]
        rowsForPct[10] = num_rows
        # use mod below for picking "rows-to-do" in case we do more than 9 trials
        # use 10 if 0 just to see (we copied 10 to 0 above)
        rowsForPct[0] = rowsForPct[10]

        expectTrainPctRightList = [0, 85.16, 88.45, 90.24, 91.27, 92.03, 92.64, 93.11, 93.48, 93.79]
        expectScorePctRightList = [0, 88.81, 91.72, 93.06, 94.02, 94.52, 95.09, 95.41, 95.77, 95.78]

        print "Creating the key of the last 10% data, for scoring"
        dataKeyTest = "rTest"
        dataKeyTrain = "rTrain"
        # start at 90% rows + 1
        # randomBitVector(size,selected)
        # randomFilter(srcFrame,rows,seed)
        # filter(srcFrame,bitVect)

        # Just for test purposes, create these using randomFilter..they overlap, so not valid for really test/train

        # FIX! too many digits (10) in the 2nd param seems to cause stack trace
        execExpr = dataKeyTest + "=randomFilter(" + key2 + "," + str(pct10) + ",12345)"
        h2o_exec.exec_expr(None, execExpr, resultKey=dataKeyTest, timeoutSecs=10)

        execExpr = dataKeyTrain + "=randomFilter(" + key2 + "," + str(rowsForPct[9]) + ",12345)"
        h2o_exec.exec_expr(None, execExpr, resultKey=dataKeyTrain, timeoutSecs=10)

        ### time.sleep(3600)
        # keep the 0 entry empty
        actualTrainPctRightList = [0]
        actualScorePctRightList = [0]
        
        for trial in range(1,10):
            # always slice from the beginning
            rowsToUse = rowsForPct[trial%10] 
            resultKey = "r" + str(trial)
            execExpr = resultKey + "=slice(" + dataKeyTrain + ",1," + str(rowsToUse) + ")"
            # execExpr = resultKey + "=slice(" + dataKeyTrain + ",1)"
            h2o_exec.exec_expr(None, execExpr, resultKey=resultKey, timeoutSecs=10)
            parseKey['destination_key'] = resultKey

            # adjust timeoutSecs with the number of trees
            # seems ec2 can be really slow
            kwargs = paramDict.copy()
            timeoutSecs = 30 + kwargs['ntree'] * 20
            start = time.time()
            # do oobe
            kwargs['out_of_bag_error_estimate'] = 1
            kwargs['model_key'] = "model_" + str(trial)
            
            rfv = h2o_cmd.runRFOnly(parseKey=parseKey, timeoutSecs=timeoutSecs, **kwargs)
            elapsed = time.time() - start
            print "RF end on ", csvPathname, 'took', elapsed, 'seconds.', \
                "%d pct. of timeout" % ((elapsed/timeoutSecs) * 100)

            oobeTrainPctRight = 100 * (1.0 - rfv['confusion_matrix']['classification_error'])
            self.assertAlmostEqual(oobeTrainPctRight, expectTrainPctRightList[trial],
                msg="OOBE: pct. right for %s pct. training not close enough %6.2f %6.2f"% \
                    ((trial*10), oobeTrainPctRight, expectTrainPctRightList[trial]), 
                delta=0.2)
            actualTrainPctRightList.append(oobeTrainPctRight)

            print "Now score on the last 10%"
            # pop the stuff from kwargs that were passing as params
            model_key = rfv['model_key']
            kwargs.pop('model_key',None)

            data_key = rfv['data_key']
            kwargs.pop('data_key',None)

            ntree = rfv['ntree']
            kwargs.pop('ntree',None)
            # scoring
            # RFView.html?
            # dataKeyTest=a5m.hex&
            # model_key=__RFModel_81c5063c-e724-4ebe-bfc1-3ac6838bc628&
            # response_variable=1&
            # ntree=50&
            # class_weights=-1%3D1.0%2C0%3D1.0%2C1%3D1.0&
            # out_of_bag_error_estimate=1&
# http://192.168.1.28:54321/GeneratePredictionsPage.html?model_key=__RFModel_0e2531bc-2552-4f65-8a4a-843031b0cb99&key=iris
# http://192.168.1.28:54321/RFView.html?data_key=iris.hex&model_key=__RFModel_0e2531bc-2552-4f65-8a4a-843031b0cb99&ntree=50&response_variable=4&class_weights=Iris-setosa%3D1.0%2CIris-versicolor%3D1.0%2CIris-virginica%3D1.0&out_of_bag_error_estimate=true&iterative_cm=true

            kwargs['iterative_cm'] = 1
            # do full scoring
            kwargs['out_of_bag_error_estimate'] = 0
            rfv = h2o_cmd.runRFView(None, dataKeyTest, model_key, ntree,
                timeoutSecs, retryDelaySecs=1, print_params=True, **kwargs)

            h2o.nodes[0].random_forest_predict(model_key=model_key, key=dataKeyTest)

            fullScorePctRight = 100 * (1.0 - rfv['confusion_matrix']['classification_error'])
            self.assertAlmostEqual(fullScorePctRight,expectScorePctRightList[trial],
                msg="Full: pct. right for scoring after %s pct. training not close enough %6.2f %6.2f"% \
                    ((trial*10), fullScorePctRight, expectScorePctRightList[trial]), 
                delta=0.2)
            actualScorePctRightList.append(fullScorePctRight)

            print "Trial #", trial, "completed", "using %6.2f" % (rowsToUse*100.0/num_rows), "pct. of all rows"

        actualDelta = [abs(a-b) for a,b in zip(expectTrainPctRightList, actualTrainPctRightList)]
        niceFp = ["{0:0.2f}".format(i) for i in actualTrainPctRightList]
        print "maybe should update with actual. Remove single quotes"  
        print"expectTrainPctRightList =", niceFp
        niceFp = ["{0:0.2f}".format(i) for i in actualDelta]
        print "actualDelta =", niceFp

        actualDelta = [abs(a-b) for a,b in zip(expectScorePctRightList, actualScorePctRightList)]
        niceFp = ["{0:0.2f}".format(i) for i in actualScorePctRightList]
        print "maybe should update with actual. Remove single quotes"  
        print "expectScorePctRightList =", niceFp
        niceFp = ["{0:0.2f}".format(i) for i in actualDelta]
        print "actualDelta =", niceFp

if __name__ == '__main__':
    h2o.unit_main()
