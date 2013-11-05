import unittest, random, sys, time, re
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import as h2i, h2o_glm, h2o_util, h2o_jobs

DO_SCORE = True
class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        # assume we're at 0xdata with it's hdfs namenode
        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(3, java_heap_GB=4)
        else:
            # all hdfs info is done thru the hdfs_config michal's ec2 config sets up?  h2o_hosts.build_cloud_with_hosts()
            h2o.build_cloud_with_hosts(3)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_NN_mnist(self):
        csvFilelist = [
            ("mnist_training.csv.gz", "mnist_testing.csv.gz",    600), 
            ("mnist_training.csv.gz", "mnist_testing.csv.gz",    600), 
            ("mnist_training.csv.gz", "mnist_testing.csv.gz",    600), 
            ("mnist_training.csv.gz", "mnist_testing.csv.gz",    600), 
        ]

        trial = 0
        for (trainCsvFilename, testCsvFilename, timeoutSecs) in csvFilelist:
            trialStart = time.time()

            # PARSE test****************************************
            testKey2 = testCsvFilename + "_" + str(trial) + ".hex"
            start = time.time()
            parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path="mnist/" + testCsvFilename, schema='put',
                hex_key=testKey2, timeoutSecs=timeoutSecs, noise=('StoreView', None))
            elapsed = time.time() - start
            print "parse end on ", testCsvFilename, 'took', elapsed, 'seconds',\
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
            print "parse result:", parseResult['destination_key']


            # PARSE train****************************************
            trainKey2 = trainCsvFilename + "_" + str(trial) + ".hex"
            start = time.time()
            parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path="mnist/" + trainCsvFilename, schema='put',
                hex_key=trainKey2, timeoutSecs=timeoutSecs, noise=('StoreView', None))
            elapsed = time.time() - start
            print "parse end on ", trainCsvFilename, 'took', elapsed, 'seconds',\
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
            print "parse result:", parseResult['destination_key']

            # NN****************************************
            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
            print "\n" + trainCsvFilename, \
                "    num_rows:", "{:,}".format(inspect['num_rows']), \
                "    num_cols:", "{:,}".format(inspect['num_cols'])

            response = inspect['num_cols'] - 1
            # up to but not including
            x = ",".join(map(str,range(response)))

            modelKey = 'a.hex'
            kwargs = {
                # this is ignore??
                'response': 0, # first column is pixel value
                # 'cols': x, # apparently no longer required? 
                'ignored_cols': None, # this is not consistent with ignored_cols_by_name
                'classification': 1,
                'validation': trainKey2,
                'activation': 'Tanh', # 'Rectifier'
                'hidden': 500, # comma separated values, or from:to:step
                'rate': 0.01,  # learning rate
                'l2': 1.0E-4, # regularization
                'epochs': 2, # how many times dataset should be iterated
                'destination_key': modelKey,
            }

            timeoutSecs = 600
            start = time.time()
            h2o.beta_features = True
            nnFirstResult = h2o_cmd.runNNet(parseResult=parseResult, timeoutSecs=timeoutSecs, noPoll=True, **kwargs)
            print "nnFirstResult:", h2o.dump_json(nnFirstResult)
            print "Hack: neural net apparently doesn't support the right polling response yet?"
            h2o_jobs.pollWaitJobs(pattern=None, errorIfCancelled=True, timeoutSecs=300, pollTimeoutSecs=10, retryDelaySecs=5)
            print "neural net end on ", trainCsvFilename, 'took', time.time() - start, 'seconds'

            # hack it!
            job_key = nnFirstResult['job_key']
            params = {'job_key': job_key, 'destination_key': modelKey}
            a = h2o.nodes[0].completion_redirect(jsonRequest="2/NeuralNetProgress.json", params=params)
            print "NeuralNetProgress:", h2o.dump_json(a)

            # print 'From hack url for neural net result:', h2o.dump_json(a)

            if DO_SCORE:
                kwargs = {
                    'max_rows': 0,
                    'response': 0, # first column is pixel value
                    # 'cols': x, # apparently no longer required? 
                    'ignored_cols': None, # this is not consistent with ignored_cols_by_name
                    'cols': None, # this is not consistent with ignored_cols_by_name
                    'classification': 1,
                    'destination_key': 'b.hex',
                    'model': modelKey,
                }
                nnScoreFirstResult = h2o_cmd.runNNetScore(key=parseResult['destination_key'], timeoutSecs=timeoutSecs, noPoll=True, **kwargs)
                h2o.beta_features = False
                print "Hack: neural net apparently doesn't support the right polling response yet?"
                h2o_jobs.pollWaitJobs(pattern=None, errorIfCancelled=True, timeoutSecs=300, pollTimeoutSecs=10, retryDelaySecs=5)


                print "neural net score end on ", trainCsvFilename, 'took', time.time() - start, 'seconds'
                print "nnScoreResult:", h2o.dump_json(nnScoreResult)

            h2o.beta_features = False

if __name__ == '__main__':
    h2o.unit_main()
