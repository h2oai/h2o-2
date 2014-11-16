import unittest, random, sys, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_rf as h2o_rf, h2o_import as h2i, h2o_exec, h2o_util, h2o_browse as h2b

# we can pass ntree thru kwargs if we don't use the "trees" parameter in runRF
# only classes 1-7 in the 55th col

# this sucks!
ALLOWED_DELTA=40
DO_MULTINOMIAL=False
paramDict = {
    'response': 'C55',
    'ntrees': 3,
    'max_depth': 30,
    'nbins': 100,
    # 'ignored_cols_by_name': "C1,C2,C6,C7,C8",
    # since comparing sorted vs not, need 100% sample to use same data for training
    # complains about no validation, which is not true, if 1.0 sample rate. make it just a little smaller
    'sample_rate': .99999,
    'classification': 1,
    'seed': '1234567890',
    'importance': 1,
    # 'mtries': 6, 
    }

class Basic(unittest.TestCase):
    def tearDown(self):
        # h2b.browseJsonHistoryAsUrlLastMatch("RF")
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init(2, java_heap_GB=7)
        h2b.browseTheCloud()

    @classmethod
    def tearDownClass(cls):
        ### h2o.sleep(800)
        h2o.tear_down_cloud()

    def rf_covtype_train_oobe(self, csvFilename, checkExpectedResults=True, expectedAuc=0.5):
        # the expected results are only for the shuffled version
        # since getting 10% samples etc of the smallish dataset will vary between 
        # shuffled and non-shuffled datasets
        importFolderPath = "standard"
        csvPathname = importFolderPath + "/" + csvFilename
        hex_key = csvFilename + ".hex"
        parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, 
            hex_key=hex_key, timeoutSecs=180)
        inspect = h2o_cmd.runInspect(key=parseResult['destination_key'])
        print "\n" + csvPathname, \
            "    numRows:", "{:,}".format(inspect['numRows']), \
            "    numCols:", "{:,}".format(inspect['numCols'])

        numCols = inspect['numCols']
        numRows = inspect['numRows']
        pct10 = int(numRows * .1)
        rowsForPct = [i * pct10 for i in range(0,11)]
        # this can be slightly less than 10%
        last10 = numRows - rowsForPct[9]
        rowsForPct[10] = numRows
        # use mod below for picking "rows-to-do" in case we do more than 9 trials
        # use 10 if 0 just to see (we copied 10 to 0 above)
        rowsForPct[0] = rowsForPct[10]

        # 0 isn't used
        expectTrainPctRightList = [0, 85.16, 88.45, 90.24, 91.27, 92.03, 92.64, 93.11, 93.48, 93.79]
        expectScorePctRightList = [0, 88.81, 91.72, 93.06, 94.02, 94.52, 95.09, 95.41, 95.77, 95.78]

        # keep the 0 entry empty
        actualTrainPctRightList = [0]
        actualScorePctRightList = [0]
        
        trial = 0
        for rowPct in [0.9]:
            trial += 1
            # Not using this now (did use it for slicing)
            rowsToUse = rowsForPct[trial%10] 
            resultKey = "r_" + csvFilename + "_" + str(trial)
            
            # just do random split for now
            dataKeyTrain = 'rTrain.hex'
            dataKeyTest = 'rTest.hex'

            response = "C55"
            h2o_cmd.createTestTrain(hex_key, dataKeyTrain, dataKeyTest, trainPercent=90, outputClass=4, 
                outputCol=numCols-1, changeToBinomial=not DO_MULTINOMIAL)
            sliceResult = {'destination_key': dataKeyTrain}

            # adjust timeoutSecs with the number of trees
            kwargs = paramDict.copy()
            kwargs['destination_key'] = "model_" + csvFilename + "_" + str(trial)
            timeoutSecs = 30 + kwargs['ntrees'] * 20
            start = time.time()
            # have to pass validation= param to avoid getting no error results (since 100% sample..DRF2 doesn't like that)
            rfv = h2o_cmd.runRF(parseResult=sliceResult, timeoutSecs=timeoutSecs, validation=dataKeyTest, **kwargs)

            elapsed = time.time() - start
            print "RF end on ", csvPathname, 'took', elapsed, 'seconds.', \
                "%d pct. of timeout" % ((elapsed/timeoutSecs) * 100)

            (error, classErrorPctList, totalScores) = h2o_rf.simpleCheckRFView(rfv=rfv, **kwargs)
            # oobeTrainPctRight = 100 * (1.0 - error)
            oobeTrainPctRight = 100 - error
            if checkExpectedResults:
                self.assertAlmostEqual(oobeTrainPctRight, expectTrainPctRightList[trial],
                    msg="OOBE: pct. right for %s pct. training not close enough %6.2f %6.2f"% \
                        ((trial*10), oobeTrainPctRight, expectTrainPctRightList[trial]), delta=ALLOWED_DELTA)
            actualTrainPctRightList.append(oobeTrainPctRight)

            print "Now score on the last 10%. Note this is silly if we trained on 100% of the data"
            print "Or sorted by output class, so that the last 10% is the last few classes"
            rf_model = rfv['drf_model']
            used_trees = rf_model['N']
            data_key = rf_model['_dataKey']
            model_key = rf_model['_key']

            rfvScoring = h2o_cmd.runScore(dataKey=dataKeyTest, modelKey=model_key, 
                vactual=response, vpredict=1, expectedAuc=expectedAuc)
            print h2o.dump_json(rfvScoring)
            h2o_rf.simpleCheckRFScore(rfv=rfvScoring, **kwargs)
            print "hello7"
            (error, classErrorPctList, totalScores) = h2o_rf.simpleCheckRFScore(rfv=rfvScoring, **kwargs)
            fullScorePctRight = 100 - error

            h2o.nodes[0].generate_predictions(model_key=model_key, data_key=dataKeyTest)

            if checkExpectedResults:
                self.assertAlmostEqual(fullScorePctRight,expectScorePctRightList[trial],
                    msg="Full: pct. right for scoring after %s pct. training not close enough %6.2f %6.2f"% \
                        ((trial*10), fullScorePctRight, expectScorePctRightList[trial]), delta=ALLOWED_DELTA)
            actualScorePctRightList.append(fullScorePctRight)

            print "Trial #", trial, "completed", "using %6.2f" % (rowsToUse*100.0/numRows), "pct. of all rows"

        actualDelta = [abs(a-b) for a,b in zip(expectTrainPctRightList, actualTrainPctRightList)]
        niceFp = ["{0:0.2f}".format(i) for i in actualTrainPctRightList]
        print "maybe should update with actual. Remove single quotes"  
        print "actualTrainPctRightList =", niceFp
        niceFp = ["{0:0.2f}".format(i) for i in actualDelta]
        print "actualDelta =", niceFp

        actualDelta = [abs(a-b) for a,b in zip(expectScorePctRightList, actualScorePctRightList)]
        niceFp = ["{0:0.2f}".format(i) for i in actualScorePctRightList]
        print "maybe should update with actual. Remove single quotes"  
        print "actualScorePctRightList =", niceFp
        niceFp = ["{0:0.2f}".format(i) for i in actualDelta]
        print "actualDelta =", niceFp

        return rfvScoring

    def test_rf_covtype_train_oobe_fvec(self):
        print "\nRun test iterations/compare with covtype.data"
        rfv1 = self.rf_covtype_train_oobe('covtype.data', checkExpectedResults=False, expectedAuc=0.95)
        (ce1, classErrorPctList, totalScores) = h2o_rf.simpleCheckRFScore(rfv=rfv1)
        # since we created a binomial output class..look at the error rate for class 1
        ce1pct1 = classErrorPctList[1]

        print "\nRun test iterations/compare with covtype.shuffled.data"
        rfv2 = self.rf_covtype_train_oobe('covtype.shuffled.data', checkExpectedResults=True, expectedAuc=0.95)
        (ce2, classErrorPctList, totalScores) = h2o_rf.simpleCheckRFScore(rfv=rfv2)
        ce2pct1 = classErrorPctList[1]

        print "\nRun test iterations/compare with covtype.sorted.data"
        rfv3 = self.rf_covtype_train_oobe('covtype.sorted.data', checkExpectedResults=False, expectedAuc=0.95)
        (ce3, classErrorPctList, totalScores) = h2o_rf.simpleCheckRFScore(rfv=rfv3)
        ce3pct1 = classErrorPctList[1]

        print "rfv3, from covtype.sorted.data"
        print "\nJsonDiff covtype.data rfv, to covtype.sorted.data rfv"
        print "rfv1:", h2o.dump_json(rfv1)
        print "rfv3:", h2o.dump_json(rfv3)
        # df = h2o_util.JsonDiff(rfv1, rfv3, with_values=True)
        df = h2o_util.JsonDiff(rfv1, rfv3)
        print "df.difference:", h2o.dump_json(df.difference)

        self.assertAlmostEqual(ce1, ce2, delta=0.5, 
            msg="classification error %s isn't close to that when sorted %s" % (ce1, ce2))
        self.assertAlmostEqual(ce1, ce3, delta=0.5, 
            msg="classification error %s isn't close to that when sorted %s" % (ce1, ce3))

        # we're doing separate test/train splits..so we're going to get variance
        # really should not do test/train split and use all the data? if we're comparing sorted or not?
        # but need the splits to be sorted or not. I think I have those files
        self.assertAlmostEqual(ce1pct1, ce2pct1, delta=10.0, 
            msg="classErrorPctList[1] %s isn't close to that when sorted %s" % (ce1pct1, ce2pct1))
        self.assertAlmostEqual(ce1pct1, ce3pct1, delta=10.0, 
            msg="classErrorPctList[1] %s isn't close to that when sorted %s" % (ce1pct1, ce3pct1))

if __name__ == '__main__':
    h2o.unit_main()
