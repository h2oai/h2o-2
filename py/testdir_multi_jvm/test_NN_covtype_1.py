import unittest, time, sys, random
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_glm, h2o_hosts, h2o_import as h2i, h2o_jobs

DO_SCORE = True
class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(3, java_heap_GB=4, base_port=54323)
        else:
            h2o_hosts.build_cloud_with_hosts(base_port=54323)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_NN_covtype_1(self):
        tryList = [
            'covtype.shuffled.90pct.sorted.data',
            'covtype.shuffled.90pct.data'
        ]
            
        importFolderPath = 'standard'
        for csvFilename in tryList:
            csvPathname = importFolderPath +  "/" + csvFilename
            hex_key = 'covtype.hex'
            parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, schema='local', hex_key=hex_key, timeoutSecs=10)
            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
            print "\n" + csvPathname, \
                "    num_rows:", "{:,}".format(inspect['num_rows']), \
                "    num_cols:", "{:,}".format(inspect['num_cols'])

            # print "WARNING: just doing the first 33 features, for comparison to ??? numbers"
            # x = ",".join(map(str,range(33)))
            x = ""

            response = 54
            modelKey = 'a.hex'
            kwargs = {
                # this is ignore??
                'response': response,
                # 'cols': x, # apparently no longer required? 
                'ignored_cols': None, # this is not consistent with ignored_cols_by_name
                'classification': 1,
                'validation': hex_key,
                'activation': 'Tanh', # 'Rectifier'
                'hidden': 500, # comma separated values, or from:to:step
                'rate': 0.01,  # learning rate
                'l2': 1.0E-4, # regularization
                'epochs': 1, # how many times dataset should be iterated
                'destination_key': modelKey,
            }

            timeoutSecs = 600
            start = time.time()
            h2o.beta_features = True
            nnFirstResult = h2o_cmd.runNNet(parseResult=parseResult, timeoutSecs=timeoutSecs, noPoll=True, **kwargs)
            print "nnFirstResult:", h2o.dump_json(nnFirstResult)
            print "Hack: neural net apparently doesn't support the right polling response yet?"
            h2o_jobs.pollWaitJobs(pattern=None, timeoutSecs=300, pollTimeoutSecs=10, retryDelaySecs=5)
            print "neural net end on ", csvPathname, 'took', time.time() - start, 'seconds'

            # hack it!
            job_key = nnFirstResult['job_key']

            # is the job finishing before polling would say it's done?
            params = {'job_key': job_key, 'destination_key': modelKey}
            a = h2o.nodes[0].completion_redirect(jsonRequest="2/NeuralNetProgress.json", params=params)

            # fake it
            ## response = {'redirect_url': "2/NeuralNetProgress.json?job_key=%s&destination_key=%s" % (job_key, modelKey)}
            ## a = h2o.nodes[0].poll_url(response, timeoutSecs=30)

            print "NeuralNetProgress:", h2o.dump_json(a)

            # print 'From hack url for neural net result:', h2o.dump_json(a)

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
                nnScoreFirstResult = h2o_cmd.runNNetScore(key=parseResult['destination_key'], timeoutSecs=timeoutSecs, noPoll=True, **kwargs)
                h2o.beta_features = False
                print "Hack: neural net apparently doesn't support the right polling response yet?"
                h2o_jobs.pollWaitJobs(pattern=None, errorIfCancelled=True, timeoutSecs=300, pollTimeoutSecs=10, retryDelaySecs=5)


                print "neural net score end on ", trainCsvFilename, 'took', time.time() - start, 'seconds'
                print "nnScoreResult:", h2o.dump_json(nnScoreResult)

            h2o.beta_features = False


if __name__ == '__main__':
    h2o.unit_main()
