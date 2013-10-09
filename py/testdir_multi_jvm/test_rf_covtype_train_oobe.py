import unittest, random, sys, time
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_rf as h2o_rf, h2o_hosts, h2o_import as h2i, h2o_exec, h2o_util

# we can pass ntree thru kwargs if we don't use the "trees" parameter in runRF
# only classes 1-7 in the 55th col
# rng DETERMINISTIC is default

# this sucks!
ALLOWED_DELTA=40.0
print "there's currently a question about whether the CM is cleared if you don't change the model name between RF's. Clear it manually using secret param"
paramDict = {
    'clear_confusion_matrix': 1,
    # FIX! does this force each jvm to see all the data? or is it conditional or ??
    'use_non_local_data': 1,
    # FIX! if there's a header, can you specify column number or column header
    'response_variable': 54,
    'class_weights': None,
    'ntree': 50,
    'out_of_bag_error_estimate': 1,
    'stat_type': 'ENTROPY',
    'depth': 2147483647, 
    'bin_limit': 10000,
    'parallel': 1,
    'ignore': "1,2,6,7,8",
    'sample': 66,
    # If I don't specify seed, every RF should get the same seed?
    # 'seed': 8977501266014959103,
    'features': 6, # why is this varying between 6 or 7 for the two datasets if not specified
    'exclusive_split_limit': 0,
    'iterative_cm': 1,
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
            "    num_rows:", "{:,}".format(inspect['num_rows']), \
            "    num_cols:", "{:,}".format(inspect['num_cols'])

        # how many rows for each pct?
        num_rows = inspect['num_rows']
        pct10 = int(num_rows * .1)
        rowsForPct = [i * pct10 for i in range(0,11)]
        # this can be slightly less than 10%
        last10 = num_rows - rowsForPct[9]
        rowsForPct[10] = num_rows
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
        
        # don't use the smaller samples..bad error rates, plus for sorted covtype, you can get just one class!
        for trial in range(8,9):
            # always slice from the beginning
            rowsToUse = rowsForPct[trial%10] 
            resultKey = "r_" + csvFilename + "_" + str(trial)
            execExpr = resultKey + " = slice(" + hex_key + ",1," + str(rowsToUse) + ")"
            h2o_exec.exec_expr(None, execExpr, resultKey=resultKey, timeoutSecs=10)
            # hack so the RF will use the sliced result
            # FIX! don't use the sliced bit..use the whole data for rf training below
            ### parseResult['destination_key'] = resultKey

            # adjust timeoutSecs with the number of trees
            # seems ec2 can be really slow
            kwargs = paramDict.copy()
            timeoutSecs = 30 + kwargs['ntree'] * 20
            # do oobe
            kwargs['out_of_bag_error_estimate'] = 1
            kwargs['model_key'] = "model_" + csvFilename + "_" + str(trial)
            # kwargs['model_key'] = "model"
            # double check the rows/cols
            inspect = h2o_cmd.runInspect(key=parseResult['destination_key'])
            h2o_cmd.infoFromInspect(inspect, "going into RF")
            
            start = time.time()
            rfv = h2o_cmd.runRF(parseResult=parseResult, timeoutSecs=timeoutSecs, **kwargs)
            elapsed = time.time() - start
            print "RF end on ", csvPathname, 'took', elapsed, 'seconds.', \
                "%d pct. of timeout" % ((elapsed/timeoutSecs) * 100)

            oobeTrainPctRight = 100 * (1.0 - rfv['confusion_matrix']['classification_error'])
            if checkExpectedResults:
                self.assertAlmostEqual(oobeTrainPctRight, expectTrainPctRightList[trial],
                    msg="OOBE: pct. right for %s pct. training not close enough %6.2f %6.2f"% \
                        ((trial*10), oobeTrainPctRight, expectTrainPctRightList[trial]), delta=ALLOWED_DELTA)
            actualTrainPctRightList.append(oobeTrainPctRight)
            


            print "Now score on the last 10%. Note this is silly if we trained on 100% of the data"
            print "Or sorted by output class, so that the last 10% is the last few classes"
            # pop the stuff from kwargs that were passing as params
            model_key = rfv['model_key']
            kwargs.pop('model_key',None)

            data_key = rfv['data_key']
            kwargs.pop('data_key',None)

            ntree = rfv['ntree']
            kwargs.pop('ntree',None)

            kwargs['iterative_cm'] = 1
            kwargs['no_confusion_matrix'] = 0

            # do full scoring
            kwargs['out_of_bag_error_estimate'] = 0
            # double check the rows/cols
            inspect = h2o_cmd.runInspect(key=dataKeyTest)
            h2o_cmd.infoFromInspect(inspect, "dataKeyTest")

            rfvScoring = h2o_cmd.runRFView(None, dataKeyTest, model_key, ntree,
                timeoutSecs, retryDelaySecs=1, print_params=True, **kwargs)

            h2o.nodes[0].generate_predictions(model_key=model_key, data_key=dataKeyTest)

            fullScorePctRight = 100 * (1.0 - rfvScoring['confusion_matrix']['classification_error'])

            if checkExpectedResults:
                self.assertAlmostEqual(fullScorePctRight,expectScorePctRightList[trial],
                    msg="Full: pct. right for scoring after %s pct. training not close enough %6.2f %6.2f"% \
                        ((trial*10), fullScorePctRight, expectScorePctRightList[trial]), delta=ALLOWED_DELTA)
            actualScorePctRightList.append(fullScorePctRight)

            print "Trial #", trial, "completed", "using %6.2f" % (rowsToUse*100.0/num_rows), "pct. of all rows"

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

    def test_rf_covtype_train_oobe(self):
        print "\nRun test iterations/compare with covtype.data"
        rfv1 = self.rf_covtype_train_oobe('covtype.data', checkExpectedResults=False)

        print "\nRun test iterations/compare with covtype.shuffled.data"
        rfv2 = self.rf_covtype_train_oobe('covtype.shuffled.data', checkExpectedResults=True)

        print "\nRun test iterations/compare with covtype.sorted.data"
        rfv3 = self.rf_covtype_train_oobe('covtype.sorted.data', checkExpectedResults=False)

        print "rfv3, from covtype.sorted.data"
        print h2o.dump_json(rfv3)
        print "\nJsonDiff covtype.data rfv, to covtype.sorted.data rfv"
        df = h2o_util.JsonDiff(rfv1, rfv3, with_values=True)
        print "df.difference:", h2o.dump_json(df.difference)
        ## self.assertEqual(len(df.difference), 0,
        ##    msg="Want 0 , not %d differences between the two rfView json responses. %s" % \
        ##        (len(df.difference), h2o.dump_json(df.difference)))
        ce1 = rfv1['confusion_matrix']['classification_error']
        ce3 = rfv3['confusion_matrix']['classification_error']
        self.assertAlmostEqual(ce1, ce3, places=3, msg="classication error %s isn't close to that when sorted %s" % (ce1, ce3))


if __name__ == '__main__':
    h2o.unit_main()
