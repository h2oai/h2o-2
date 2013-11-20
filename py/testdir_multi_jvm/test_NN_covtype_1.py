import unittest, time, sys, random
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_glm, h2o_hosts, h2o_import as h2i, h2o_jobs, h2o_gbm

DO_SCORE = True
DO_POLL = True
class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        localhost = h2o.decide_if_localhost()
        if (localhost):
            # h2o.build_cloud(3, java_heap_GB=4, base_port=54323)
            h2o.build_cloud(3, java_heap_GB=12, base_port=54323)
        else:
            h2o_hosts.build_cloud_with_hosts(base_port=54323)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_NN_covtype_1(self):
        h2o.beta_features = True
        tryList = [
            ('covtype.shuffled.90pct.sorted.data', 'covtype.shuffled.10pct.sorted.data'),
            ('covtype.shuffled.90pct.data', 'covtype.shuffled.10pct.data')
        ]
            
        importFolderPath = 'standard'
        for trainFilename, testFilename in tryList:
            # Parse Train********************************
            trainPathname = importFolderPath +  "/" + trainFilename
            trainHexKey = 'covtype90.hex'
            trainParseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=trainPathname, schema='local', hex_key=trainHexKey, timeoutSecs=10)
            inspect = h2o_cmd.runInspect(None, trainParseResult['destination_key'])
            print "\n" + trainPathname, \
                "    numRows:", "{:,}".format(inspect['numRows']), \
                "    numCols:", "{:,}".format(inspect['numCols'])
            # Parse Test********************************
            testPathname = importFolderPath +  "/" + testFilename
            testHexKey = 'covtype10.hex'
            testParseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=testPathname, schema='local', hex_key=testHexKey, timeoutSecs=10)

            # NN Train********************************
            x = ""
            response = 'C54'
            modelKey = 'a.hex'
            kwargs = {
                # this is ignore??
                'response': response,
                # 'cols': x, # apparently no longer required? 
                'ignored_cols': None, # this is not consistent with ignored_cols_by_name
                'classification': 1,
                'validation': testHexKey,
                'activation': 'Tanh', # 'Rectifier'
                'hidden': 500, # comma separated values, or from:to:step
                'rate': 0.01,  # learning rate
                'l2': 1.0E-2, # regularization
                # can overfit the training data
                'epochs': 5, # how many times dataset should be iterated
                'destination_key': modelKey,
            }

            timeoutSecs = 600
            start = time.time()
            nnResult = h2o_cmd.runNNet(parseResult=trainParseResult, timeoutSecs=timeoutSecs, noPoll=not DO_POLL, **kwargs)
            print "nnResult:" if DO_POLL else "first nnResult:", h2o.dump_json(nnResult)

            if not DO_POLL:
                h2o_jobs.pollWaitJobs(pattern=None, timeoutSecs=300, pollTimeoutSecs=10, retryDelaySecs=5)
                # hack it!
                job_key = nnResult['job_key']
                params = {'job_key': job_key, 'destination_key': modelKey}
                a = h2o.nodes[0].completion_redirect(jsonRequest="2/NeuralNetProgress.json", params=params)
                print "NeuralNetProgress:", h2o.dump_json(a)

            print "neural net end on ", trainPathname, 'took', time.time() - start, 'seconds'

            # NN Score********************************
            if DO_SCORE:
                kwargs = {
                    'max_rows': 0,
                    'response': response,
                    # 'cols': x, # apparently no longer required? 
                    'ignored_cols': None, # this is not consistent with ignored_cols_by_name
                    'cols': None, # this is not consistent with ignored_cols_by_name
                    'classification': 1,
                    'destination_key': 'b.hex',
                    'model': modelKey,
                }
                # doesn't need polling?
                nnScoreResult = h2o_cmd.runNNetScore(key=testParseResult['destination_key'], 
                    timeoutSecs=timeoutSecs, noPoll=True, **kwargs)

                print "neural net score end on ", testPathname, 'took', time.time() - start, 'seconds'
                # print "nnScoreResult:", h2o.dump_json(nnScoreResult)
                cm = nnScoreResult['confusion_matrix']
                mean_square_error = nnScoreResult['mean_square_error']
                classification_error = nnScoreResult['classification_error']

                # These will move into the h2o_gbm.py
                pctWrong = h2o_gbm.pp_cm_summary(cm);
                print "\nTest\n==========\n"
                print "classification_error:", classification_error
                print "mean_square_error:", mean_square_error
                print h2o_gbm.pp_cm(cm)

if __name__ == '__main__':
    h2o.unit_main()
