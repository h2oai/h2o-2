import unittest, random, sys, time
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_rf as h2o_rf, h2o_hosts, h2o_import as h2i, h2o_exec, h2o_util

# we can pass ntree thru kwargs if we don't use the "trees" parameter in runRF
# only classes 1-7 in the 55th col

# this sucks!
ALLOWED_DELTA=40.0
    'response': 54,
    'ntrees': 10,
    'max_depth': 10,
    'bin_limit': 10,
    'ignore': "C1,C2,C6,C7,C8",
    'sample': 0.66,
    'mtries': 6, 
    }

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(2, java_heap_GB=7)
        else:
            h2o_hosts.build_cloud_with_hosts(java_heap_GB=10)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def rf_covtype_train_oobe(self, csvFilename, checkExpectedResults=True):
        # the expected results are only for the shuffled version
        # since getting 10% samples etc of the smallish dataset will vary between 
        # shuffled and non-shuffled datasets
        importFolderPath = "standard"
        csvPathname = importFolderPath + "/" + csvFilename
        hex_key = csvFilename + ".hex"
        print "\nUsing header=0 on", csvFilename
        parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, hex_key=hex_key,
            header=0, timeoutSecs=180)

        inspect = h2o_cmd.runInspect(key=parseResult['destination_key'])
        print "\n" + csvPathname, \
            "    numRows:", "{:,}".format(inspect['numRows']), \
            "    numCols:", "{:,}".format(inspect['numCols'])

        # how many rows for each pct?
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

        print "Creating the key of the last 10% data, for scoring"
        dataKeyTest = "rTest"
        # start at 90% rows + 1
        execExpr = dataKeyTest + " = slice(" + hex_key + "," + str(rowsForPct[9]+1) + ")"
        h2o_exec.exec_expr(None, execExpr, resultKey=dataKeyTest, timeoutSecs=10)

        # keep the 0 entry empty
        actualTrainPctRightList = [0]
        actualScorePctRightList = [0]
        
        trial = 0
        for rowPct in range(0.9):
            trial += 1
            # always slice from the beginning
            rowsToUse = rowsForPct[trial%10] 
            resultKey = "r_" + csvFilename + "_" + str(trial)

            # will have to live with random extract. will create variance
            execExpr = "a.hex=runif(%s); " % hex_key + "b.hex=[a<%s,]" % rowPct
            h2o_exec.exec_expr(None, execExpr, resultKey='b.hex', timeoutSecs=10)
            inspect = h2o_cmd.runInspect(key=b)
            h2o_cmd.infoFromInspect(inspect, "going into RF")

            kwargs['destination_key'] = "model_" + csvFilename + "_" + str(trial)
            
            sliceResult['destination_key'] = 'b.hex'
            # adjust timeoutSecs with the number of trees
            # seems ec2 can be really slow
            kwargs = paramDict.copy()
            timeoutSecs = 30 + kwargs['ntree'] * 20
            start = time.time()
            rfv = h2o_cmd.runRF(parseResult=sliceResult, timeoutSecs=timeoutSecs, **kwargs)

            elapsed = time.time() - start
            print "RF end on ", csvPathname, 'took', elapsed, 'seconds.', \
                "%d pct. of timeout" % ((elapsed/timeoutSecs) * 100)

            (error, classErrorPctList, totalScores) = h2o_rf.simpleCheckRFView(rfv=rfView)
            oobeTrainPctRight = 100 * (1.0 - error)
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
            model_key = rf_model['_selfKey']

            # double check the rows/cols
            inspect = h2o_cmd.runInspect(key=dataKeyTest)
            h2o_cmd.infoFromInspect(inspect, "dataKeyTest")

            rfvScoring = h2o_cmd.runRFView(None, dataKeyTest, model_key, ntree,
                timeoutSecs, retryDelaySecs=1, print_params=True, **kwargs)
            (error, classErrorPctList, totalScores) = h2o_rf.simpleCheckRFView(rfv=rfScoring)
            fullScorePctRight = 100 * (1.0 - error)

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

        # return the last rfv done during training
        return rfv

    def test_rf_covtype_train_oobe_fvec(self):
        h2o.beta_features = True
        print "\nRun test iterations/compare with covtype.data"
        rfv1 = self.rf_covtype_train_oobe('covtype.data', checkExpectedResults=False)
        (ce1, classErrorPctList, totalScores) = h2o_rf.simpleCheckRFView(rfv=rfv1)

        print "\nRun test iterations/compare with covtype.shuffled.data"
        rfv2 = self.rf_covtype_train_oobe('covtype.shuffled.data', checkExpectedResults=True)
        (ce2, classErrorPctList, totalScores) = h2o_rf.simpleCheckRFView(rfv=rfv2)

        print "\nRun test iterations/compare with covtype.sorted.data"
        rfv3 = self.rf_covtype_train_oobe('covtype.sorted.data', checkExpectedResults=False)
        (ce3, classErrorPctList, totalScores) = h2o_rf.simpleCheckRFView(rfv=rfv3)

        print "rfv3, from covtype.sorted.data"
        print h2o.dump_json(rfv3)
        print "\nJsonDiff covtype.data rfv, to covtype.sorted.data rfv"
        df = h2o_util.JsonDiff(rfv1, rfv3, with_values=True)
        print "df.difference:", h2o.dump_json(df.difference)

        self.assertAlmostEqual(ce1, ce2, places=3, msg="classication error %s isn't close to that when sorted %s" % (ce1, ce2))
        self.assertAlmostEqual(ce1, ce2, places=3, msg="classication error %s isn't close to that when sorted %s" % (ce1, ce2))


if __name__ == '__main__':
    h2o.unit_main()
