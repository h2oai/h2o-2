import unittest, time, sys, random, json
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_rf, h2o_hosts, h2o_util
import h2o_browse as h2b
import h2o_jobs

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED, localhost
        SEED = h2o.setup_random_seed()

        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(1)
        else:
            h2o_hosts.build_cloud_with_hosts(1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_rf_big1_nopoll(self):
        csvFilename = 'hhp_107_01.data.gz'
        csvPathname = h2o.find_file("smalldata/" + csvFilename)
        key2 = csvFilename + ".hex"
        
        print "\n" + csvPathname

        parseKey = h2o_cmd.parseFile(csvPathname=csvPathname, key2=key2, timeoutSecs=15)
        rfViewInitial = []
        # dispatch multiple jobs back to back
        for jobDispatch in range(100):
            start = time.time()
            kwargs = {}
            model_key = 'RF_model' + str(jobDispatch)
            kwargs['ntree'] = 7
            kwargs['seed'] = random.randint(0, sys.maxint)

            # FIX! what model keys do these get?
            randomNode = h2o.nodes[random.randint(0,len(h2o.nodes)-1)]
            h2o_cmd.runRFOnly(node=randomNode, parseKey=parseKey, model_key=model_key, timeoutSecs=300, noPoll=True, **kwargs)
            # FIX! are these already in there?
            rfView = {}
            rfView['data_key'] = key2
            rfView['model_key'] = model_key
            rfView['ntree'] = kwargs['ntree']
            rfViewInitial.append(rfView)

            print "rf job dispatch end on ", csvPathname, 'took', time.time() - start, 'seconds'
            print "\njobDispatch #", jobDispatch

        h2o_jobs.pollWaitJobs(pattern='RF_model', timeoutSecs=300, pollTimeoutSecs=10, retryDelaySecs=5)

        # we saved the initial response?
        # if we do another poll they should be done now, and better to get it that 
        # way rather than the inspect (to match what simpleCheckGLM is expected
        first = None
        print "rfViewInitial", rfViewInitial
        for rfView in rfViewInitial:
            print "Checking completed job:", rfView
            print "rfView", h2o.dump_json(rfView)
            data_key = rfView['data_key']
            model_key = rfView['model_key']
            ntree = rfView['ntree']
            # a = h2o.nodes[0].random_forest_view(data_key, model_key, noPoll=True)
            print "Temporary hack: need to do two rf views minimum, to complete a RF (confusion matrix creation)"
            # allow it to poll to complete
            rfViewResult = h2o_cmd.runRFView(None, data_key, model_key, ntree=ntree, timeoutSecs=60, noPoll=False)
            if first is None: # we'll use this to compare the others
                first = rfViewResult.copy()
                firstModelKey = model_key
                print "first", h2o.dump_json(first)
            else:
                print "Comparing", model_key, "to", firstModelKey
                df = h2o_util.JsonDiff(rfViewResult, first, vice_versa=True, with_values=True)

                print "df.difference:", h2o.dump_json(df.difference)

if __name__ == '__main__':
    h2o.unit_main()
