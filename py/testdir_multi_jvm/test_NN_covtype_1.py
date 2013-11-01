import unittest, time, sys, random
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_glm, h2o_hosts, h2o_import as h2i, h2o_jobs

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(3, java_heap_GB=10, base_port=54323)
        else:
            h2o_hosts.build_cloud_with_hosts(base_port=54323)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_NN_covtype_1(self):
        csvFilename = 'covtype.data'
        csvPathname = 'UCI/UCI-large/covtype/' + csvFilename
        hex_key = 'covtype.hex'
        parseResult = h2i.import_parse(bucket='datasets', path=csvPathname, schema='put', hex_key=hex_key, timeoutSecs=10)
        inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
        print "\n" + csvPathname, \
            "    num_rows:", "{:,}".format(inspect['num_rows']), \
            "    num_cols:", "{:,}".format(inspect['num_cols'])

        print "WARNING: just doing the first 33 features, for comparison to ??? numbers"
        x = ",".join(map(str,range(33)))

        response = 54
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
            'destination_key': 'a.hex',
        }

        timeoutSecs = 600
        start = time.time()
        h2o.beta_features = True
        nnResult = h2o_cmd.runNNet(parseResult=parseResult, timeoutSecs=timeoutSecs, noPoll=True, **kwargs)
        h2o.beta_features = False
        print "Hack: neural net apparently doesn't support the right polling response yet?"
        h2o_jobs.pollWaitJobs(pattern=None, timeoutSecs=300, pollTimeoutSecs=10, retryDelaySecs=5)

        print "FIX! need to add something that looks at the neural net result here?"
        print "neural net end on ", csvPathname, 'took', time.time() - start, 'seconds'
        print "nnResult:", h2o.dump_json(nnResult)

if __name__ == '__main__':
    h2o.unit_main()
