import unittest, time, sys, random, json
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_rf, h2o_util, h2o_import as h2i
import h2o_browse as h2b
import h2o_jobs


OVERWRITE_RF_MODEL = True

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()

        h2o.init(1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_rf_big1_overwrite_model_fvec(self):
        csvFilename = 'hhp_107_01.data.gz'
        hex_key = csvFilename + ".hex"
        print "\n" + csvFilename
        parseResult = h2i.import_parse(bucket='smalldata', path=csvFilename, 
            hex_key=hex_key, timeoutSecs=60, schema='put')
        firstRfView = None
        # dispatch multiple jobs back to back
        for jobDispatch in range(3):
            start = time.time()
            kwargs = {}
            if OVERWRITE_RF_MODEL:
                print "Since we're overwriting here, we have to wait for each to complete noPoll=False"
                model_key = 'RF_model'
            else:
                model_key = 'RF_model' + str(jobDispatch)

            print "Change the number of trees, while keeping the rf model key name the same"
            print "Checks that we correctly overwrite previous rf model"
            if OVERWRITE_RF_MODEL:
                kwargs['ntrees'] = 1 + jobDispatch
            else:
                kwargs['ntrees'] = 1
                # don't change the seed if we're overwriting the model. It should get 
                # different results just from changing the tree count
                kwargs['seed'] = random.randint(0, sys.maxint)

            # FIX! what model keys do these get?
            randomNode = h2o.nodes[random.randint(0, len(h2o.nodes)-1)]
            h2o_cmd.runRF(node=randomNode, parseResult=parseResult, destination_key=model_key, timeoutSecs=300,
                 noPoll=True, **kwargs)
            # FIX! are these already in there?
            rfView = {}
            rfView['_dataKey'] = hex_key
            rfView['_key'] = model_key

            print "rf job dispatch end on ", csvFilename, 'took', time.time() - start, 'seconds'
            print "\njobDispatch #", jobDispatch

            # we're going to compare rf results to previous as we go along (so we save rf view results
            h2o_jobs.pollWaitJobs(pattern='RF_model', timeoutSecs=300, pollTimeoutSecs=10, retryDelaySecs=5)
    
            # In this test we're waiting after each one, so we can save the RFView results for comparison to future
            print "Checking completed job:", rfView
            print "rfView", h2o.dump_json(rfView)
            data_key = rfView['_dataKey']
            model_key = rfView['_key']
            print "Temporary hack: need to do two rf views minimum, to complete a RF (confusion matrix creation)"
            # allow it to poll to complete
            rfViewResult = h2o_cmd.runRFView(None, data_key, model_key, timeoutSecs=60, noPoll=False)
            if firstRfView is None: # we'll use this to compare the others
                firstRfView = rfViewResult.copy()
                firstModelKey = model_key
                print "firstRfView", h2o.dump_json(firstRfView)
            else:
                print "Comparing", model_key, "to", firstModelKey
                df = h2o_util.JsonDiff(rfViewResult, firstRfView, vice_versa=True, with_values=True)
                print "df.difference:", h2o.dump_json(df.difference)
                self.assertGreater(len(df.difference), 29, 
                    msg="Want >=30 , not %d differences between the two rfView json responses. %s" % \
                        (len(df.difference), h2o.dump_json(df.difference)))
                

if __name__ == '__main__':
    h2o.unit_main()
