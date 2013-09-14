import unittest
import random, sys, time, re
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import2 as h2i, h2o_glm, h2o_util, h2o_rf, h2o_jobs as h2j

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GBM_parseTrain_Acovtype200(self):
        h2o.beta_features = False
        bucket = 'home-0xdiag-datasets'
        (importFolderPath,csvFilename,trainKey,timeoutSecs,vresponse) = ('standard', 'covtype200x.data', 'covtype.hex', 1800, 54)
        start = time.time()
        parseResult = h2i.import_parse(bucket=bucket, path=importFolderPath + "/" + csvFilename,
            hex_key=trainKey, timeoutSecs=timeoutSecs)
        elapsed = time.time() - start
        print "parse end on ", csvFilename, 'took', elapsed, 'seconds',\
            "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
        print "parse result:", parseResult['destination_key']

        # GBM (train)****************************************
        params = {
            'destination_key': "GBMKEY",
            'learn_rate':.1,
            'ntrees':5,
            'max_depth':1,
            'min_rows':1,
            'vresponse':vresponse
        }
        print "Using these parameters for GBM: ", params
        kwargs = params.copy()
        h2o.beta_features = True
        #noPoll -> False when GBM finished
        GBMResult = h2o_cmd.runGBM(parseResult=parseResult, noPoll=True,timeoutSecs=timeoutSecs,**kwargs)
        h2j.pollWaitJobs(pattern="GBMKEY",timeoutSecs=1800,pollTimeoutSecs=1800)
        #print "GBM training completed in", GBMResult['python_elapsed'], "seconds.", \
        #    "%f pct. of timeout" % (GBMResult['python_%timeout'])
        GBMView = h2o_cmd.runGBMView(model_key='GBMKEY')
        print GBMView['gbm_model']['errs']

    def test_GBM_parseTrain_Zallyears(self):
        h2o.beta_features = False
        bucket = 'home-0xdiag-datasets'
        (importFolderPath,csvFilename,trainKey,timeoutSecs,vresponse) = ('standard', 'allyears.csv', 'allyears.hex',1800,'IsArrDelayed')
        start = time.time()
        parseResult = h2i.import_parse(bucket=bucket, path=importFolderPath + "/" + csvFilename,
            hex_key=trainKey, timeoutSecs=timeoutSecs)
        elapsed = time.time() - start
        print "parse end on ", csvFilename, 'took', elapsed, 'seconds',\
            "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
        print "parse result:", parseResult['destination_key']

        # GBM (train)****************************************
        params = {
            'destination_key': "GBMKEY",
            'learn_rate':.1,
            'ntrees':10,
            'max_depth':8,
            'min_rows':1,
            'vresponse':vresponse
        }
        print "Using these parameters for GBM: ", params
        kwargs = params.copy()
        h2o.beta_features = True
        #noPoll -> False when GBM finished
        GBMResult = h2o_cmd.runGBM(parseResult=parseResult, noPoll=True,timeoutSecs=timeoutSecs,**kwargs)
        h2j.pollWaitJobs(pattern="GBMKEY",timeoutSecs=1800,pollTimeoutSecs=1800)
        #print "GBM training completed in", GBMResult['python_elapsed'], "seconds.", \
        #    "%f pct. of timeout" % (GBMResult['python_%timeout'])
        GBMView = h2o_cmd.runGBMView(model_key='GBMKEY')
        print GBMView['gbm_model']['errs']

    def test_GBM_parseTrain_Cnflxf95(self):
        h2o.beta_features = False
        bucket = 'home-0xdiag-datasets'
        (importFolderPath,csvFilename,trainKey,timeoutSecs,vresponse) = ('manyfiles-nflx-gz', 'file_95.dat.gz', 'nflx.hex',1800,541)
        start = time.time()
        parseResult = h2i.import_parse(bucket=bucket, path=importFolderPath + "/" + csvFilename,
            hex_key=trainKey, timeoutSecs=timeoutSecs)
        elapsed = time.time() - start
        print "parse end on ", csvFilename, 'took', elapsed, 'seconds',\
            "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
        print "parse result:", parseResult['destination_key']

        # GBM (train)****************************************
        params = { 
            'destination_key': "GBMKEY",
            'learn_rate':.1,
            'ntrees':10,
            'max_depth':8,
            'min_rows':1,
            'vresponse':vresponse
        }
        print "Using these parameters for GBM: ", params
        kwargs = params.copy()
        h2o.beta_features = True
        #noPoll -> False when GBM finished
        GBMResult = h2o_cmd.runGBM(parseResult=parseResult, noPoll=True,timeoutSecs=timeoutSecs,**kwargs)
        h2j.pollWaitJobs(pattern="GBMKEY",timeoutSecs=1800,pollTimeoutSecs=1800)
        #print "GBM training completed in", GBMResult['python_elapsed'], "seconds.", \
        #    "%f pct. of timeout" % (GBMResult['python_%timeout'])
        GBMView = h2o_cmd.runGBMView(model_key='GBMKEY')
        print GBMView['gbm_model']['errs']

    def test_GBM_parseTrain_Bmnist(self):
        h2o.beta_features = False
        #folderpath, filename, keyname, timeout
        print "Trying with mnist8m data"
        bucket = 'home-0xdiag-datasets'
        (importFolderPath,csvFilename,trainKey,timeoutSecs,vresponse) = ('mnist', 'mnist8m.csv', 'mnist8m.hex',1800,0)
        start = time.time()
        parseResult = h2i.import_parse(bucket=bucket, path=importFolderPath + "/" + csvFilename,
            hex_key=trainKey, timeoutSecs=timeoutSecs)
        elapsed = time.time() - start
        print "parse end on ", csvFilename, 'took', elapsed, 'seconds',\
            "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
        print "parse result:", parseResult['destination_key']

        # GBM (train)****************************************
        params = { 
            'destination_key': "GBMKEY",
            'learn_rate':.1,
            'ntrees':10,
            'max_depth':8,
            'min_rows':1,
            'vresponse':vresponse
        }
        print "Using these parameters for GBM: ", params
        kwargs = params.copy()
        h2o.beta_features = True
        #noPoll -> False when GBM finished
        GBMResult = h2o_cmd.runGBM(parseResult=parseResult, noPoll=True,timeoutSecs=timeoutSecs,**kwargs)
        h2j.pollWaitJobs(pattern="GBMKEY",timeoutSecs=1800,pollTimeoutSecs=1800)
        #print "GBM training completed in", GBMResult['python_elapsed'], "seconds.", \
        #    "%f pct. of timeout" % (GBMResult['python_%timeout'])
        GBMView = h2o_cmd.runGBMView(model_key='GBMKEY')
        print GBMView['gbm_model']['errs']
            
    def test_GBM_parseTrain_Yallyears2k(self):
        h2o.beta_features = False
        bucket = 'home-0xdiag-datasets'
        (importFolderPath,csvFilename,trainKey,timeoutSecs,vresponse) = ('standard', 'allyears2k.csv', 'allyears2k.hex',1800,'IsArrDelayed')
        start = time.time()
        parseResult = h2i.import_parse(bucket=bucket, path=importFolderPath + "/" + csvFilename,
            hex_key=trainKey, timeoutSecs=timeoutSecs)
        elapsed = time.time() - start
        print "parse end on ", csvFilename, 'took', elapsed, 'seconds',\
            "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
        print "parse result:", parseResult['destination_key']

        # GBM (train)****************************************
        params = { 
            'destination_key': "GBMKEY",
            'learn_rate':.1,
            'ntrees':10,
            'max_depth':8,
            'min_rows':1,
            'vresponse':vresponse
        }   
        print "Using these parameters for GBM: ", params
        kwargs = params.copy()
        h2o.beta_features = True
        #noPoll -> False when GBM finished
        GBMResult = h2o_cmd.runGBM(parseResult=parseResult, noPoll=True,timeoutSecs=timeoutSecs,**kwargs)
        h2j.pollWaitJobs(pattern="GBMKEY",timeoutSecs=1800,pollTimeoutSecs=1800)
        #print "GBM training completed in", GBMResult['python_elapsed'], "seconds.", \
        #    "%f pct. of timeout" % (GBMResult['python_%timeout'])
        GBMView = h2o_cmd.runGBMView(model_key='GBMKEY')
        print GBMView['gbm_model']['errs'] 

if __name__ == '__main__':
    h2o.unit_main()

