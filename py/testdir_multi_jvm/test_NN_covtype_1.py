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
        parseResult = h2i.import_parse(bucket='datasets', path=csvPathname, schema='put', timeoutSecs=10)
        inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
        print "\n" + csvPathname, \
            "    num_rows:", "{:,}".format(inspect['num_rows']), \
            "    num_cols:", "{:,}".format(inspect['num_cols'])

        print "WARNING: just doing the first 33 features, for comparison to ??? numbers"
        x = ",".join(map(str,range(33)))

        response = 54
        kwargs = {
            # this is ignore??
            'cols': x, # apparently required? 
            'response': response,
            'activation': 'Tanh',
            'hidden': 500,
            'rate': 0.01,
            'l2': 1.0E-4,
            'epochs': 100,
            'destination_key': 'a.hex',
        }

        timeoutSecs = 600
        start = time.time()
        nnResult = h2o_cmd.runNNet(parseResult=parseResult, timeoutSecs=timeoutSecs, noPoll=True, **kwargs)
        print "Hack: neural net apparently doesn't support the right polling response yet?"
        h2o_jobs.pollWaitJobs(pattern=None, timeoutSecs=300, pollTimeoutSecs=10, retryDelaySecs=5)

        print "neural net end on ", csvPathname, 'took', time.time() - start, 'seconds'

if __name__ == '__main__':
    h2o.unit_main()
