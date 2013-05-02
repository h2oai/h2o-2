import os, json, unittest, time, shutil, sys, random
sys.path.extend(['.','..','py'])


import h2o, h2o_cmd, h2o_rf, h2o_hosts
import h2o_browse as h2b
import h2o_jobs

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
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
        rfView = {}
        # dispatch multiple jobs back to back
        for jobDispatch in range(25):
            start = time.time()
            kwargs = {}
            model_key = 'RF_model' + str(jobDispatch)
            # FIX! what model keys do these get?
            randomNode = h2o.nodes[random.randint(0,len(h2o.nodes)-1)]
            h2o_cmd.runRFOnly(node=randomNode, parseKey=parseKey, model_key=model_key, timeoutSecs=300, noPoll=True, **kwargs)

            print "rfView:", h2o.dump_json(rfView)
            # FIX! are these already in there?
            rfView['data_key'] = key2
            rfView['model_key'] = model_key
            rfViewInitial.append(rfView)
            print "rf job dispatch end on ", csvPathname, 'took', time.time() - start, 'seconds'
            print "\njobDispatch #", jobDispatch

        h2o_jobs.pollWaitJobs(pattern='RF_model', timeoutSecs=180, pollTimeoutSecs=120, retryDelaySecs=5)

        # we saved the initial response?
        # if we do another poll they should be done now, and better to get it that 
        # way rather than the inspect (to match what simpleCheckGLM is expected
        for rfView in rfViewInitial:
            print "Checking completed job, with no polling:", rfView
            print "rfView", h2o.dump_json(rfView)
            data_key = rfView['data_key']
            model_key = rfView['model_key']
            a = h2o.nodes[0].random_forest_view(data_key, model_key, noPoll=True)
            h2o_rf.simpleCheckRFView(None, a)

if __name__ == '__main__':
    h2o.unit_main()
