import unittest, random, sys, time
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_rf as h2o_rf, h2o_hosts, h2o_import as h2i, h2o_exec, h2o_jobs

# we can pass ntree thru kwargs if we don't use the "trees" parameter in runRF
# only classes 1-7 in the 55th col
# rng DETERMINISTIC is default
paramDict = {
    'response': 'C54',
    'cols': None,
    'ignored_cols_by_name': 'C1,C2,C6,C7,C8',
    'classification': None,
    'validation': None,
    'ntrees': 10,
    'max_depth': None,
    'min_rows': None,
    'nbins': 10000,
    'mtries': None,
    'sample_rate': 0.66,
    'seed': None,

    }

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(node_count=1, java_heap_GB=10)
        else:
            h2o_hosts.build_cloud_with_hosts(node_count=1, java_heap_GB=10)
        h2o.beta_features = True # fvec

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_rf_covtype_fvec(self):
        importFolderPath = "standard"
        csvFilename = 'covtype.data'
        csvPathname = importFolderPath + "/" + csvFilename
        hex_key = csvFilename + ".hex"

        print "\nUsing header=0 on the normal covtype.data"
        parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, hex_key=hex_key,
            header=0, timeoutSecs=180)
        inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])

        rfViewInitial = []
        for jobDispatch in range(1):
            # adjust timeoutSecs with the number of trees
            # seems ec2 can be really slow
            paramDict['destination_key'] = 'RFModel_' + str('jobDispatch')
            kwargs = paramDict.copy()
            timeoutSecs = 30 + kwargs['ntrees'] * 20

            start = time.time()
            rfResult = h2o_cmd.runRF(parseResult=parseResult, timeoutSecs=timeoutSecs, noPoll=True, rfView=False, **kwargs)
            elapsed = time.time() - start
            print "RF dispatch end on ", csvPathname, 'took', elapsed, 'seconds.', \
                "%d pct. of timeout" % ((elapsed/timeoutSecs) * 100)

            print h2o.dump_json(rfResult)
            # FIX! are these already in there?
            rfView = {}
            rfView['data_key'] = hex_key
            rfView['model_key'] = kwargs['destination_key']
            rfView['ntrees'] = kwargs['ntrees']
            rfViewInitial.append(rfView)

            print "rf job dispatch end on ", csvPathname, 'took', time.time() - start, 'seconds'
            print "\njobDispatch #", jobDispatch

            h2o_jobs.pollWaitJobs(pattern='RF_model', timeoutSecs=180, pollTimeoutSecs=120, retryDelaySecs=5)


        # we saved the initial response?
        # if we do another poll they should be done now, and better to get it that 
        # way rather than the inspect (to match what simpleCheckGLM is expected
        print "rfViewInitial", rfViewInitial
        for rfView in rfViewInitial:
            print "Checking completed job:", rfView
            print "rfView", h2o.dump_json(rfView)
            data_key = rfView['data_key']
            model_key = rfView['model_key']
            ntrees = rfView['ntrees']
            rfView = rfViewResult = h2o_cmd.runRFView(None, data_key, model_key, timeoutSecs=60, noPoll=True)
            h2o_jobs.pollWaitJobs(timeoutSecs=300, pollTimeoutSecs=300, retryDelaySecs=5)


            # FIX! should update this expected classification error
            (classification_error, classErrorPctList, totalScores) = h2o_rf.simpleCheckRFView(rfv=rfView, ntree=ntrees)
            self.assertAlmostEqual(classification_error, 0.03, delta=0.5, msg="Classification error %s differs too much" % classification_error)
            predict = h2o.nodes[0].generate_predictions(model_key=model_key, data_key=data_key)


if __name__ == '__main__':
    h2o.unit_main()
