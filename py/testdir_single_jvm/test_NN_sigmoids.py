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
        csvFilename_train = 'sumsigmoids.csv'
        csvPathname_train = 'neural/' + csvFilename_train
        csvFilename_test  = 'sumsigmoids_test.csv'
        csvPathname_test  = 'neural/' + csvFilename_test
        hex_key = 'sigmoids_train.hex'
        validation_key = 'sigmoids_test.hex'
        parseResult  = h2i.import_parse(bucket='smalldata', path=csvPathname_train, schema='local', hex_key=hex_key, timeoutSecs=10)
        parseResultV = h2i.import_parse(bucket='smalldata', path=csvPathname_test, schema='local', hex_key=validation_key, timeoutSecs=30)
        inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
        print "\n" + csvPathname_train, \
            "    num_rows:", "{:,}".format(inspect['num_rows']), \
            "    num_cols:", "{:,}".format(inspect['num_cols'])

        hidden = 1 #number of hidden units
        response = 'Y'
        
        kwargs = {
            'ignored_cols'    : None,
            'response'        : response,
            'activation'      : 'Tanh',
            'hidden'          : hidden,
            'rate'            : 0.01,
            'l2'              : 0.0005,
            'epochs'          : 10,
            'destination_key' : 'nn'+str(hidden)+'.hex',
            'validation'      : validation_key,
        }

        timeoutSecs = 600
        start = time.time()
        h2o.beta_features = True
        nnResult = h2o_cmd.runNNet(parseResult=parseResult, timeoutSecs=timeoutSecs, noPoll=True, **kwargs)
        h2o.beta_features = False
        print "Hack: neural net apparently doesn't support the right polling response yet?"
        h2o_jobs.pollWaitJobs(pattern=None, timeoutSecs=300, pollTimeoutSecs=10, retryDelaySecs=5)

        #print "FIX! need to add something that looks at the neural net result here?"
        #print "neural net end on ", csvPathname, 'took', time.time() - start, 'seconds'

if __name__ == '__main__':
    h2o.unit_main()
