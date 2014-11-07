import unittest, time, sys, random
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_glm, h2o_browse as h2b, h2o_import as h2i

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        # assume we're at 0xdata with it's hdfs namenode
        h2o.init(1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GLM_allstate_s3n_thru_hdfs(self):
        bucket = 'home-0xdiag-datasets'
        importFolderPath = 'allstate'
        csvFilename = "train_set.csv"
        csvPathname = importFolderPath + "/" + csvFilename
        timeoutSecs = 500
        trialMax = 3
        for trial in range(trialMax):
            trialStart = time.time()
            hex_key = csvFilename + "_" + str(trial) + ".hex"

            start = time.time()
            parseResult = h2i.import_parse(bucket=bucket, path=csvPathname, schema='s3n', hex_key=hex_key,
                timeoutSecs=timeoutSecs, retryDelaySecs=10, pollTimeoutSecs=60)
            elapsed = time.time() - start
            print "parse end on ", hex_key, 'took', elapsed, 'seconds',\
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
            print "parse result:", parseResult['destination_key']

            kwargs = {
                # allstate claim last col
                'y': 34,
                'case_mode': '>',
                'case': 0,

                'family': 'binomial',
                'link': 'logit',
                'n_folds': 2,
                'max_iter': 8,
                'beta_epsilon': 1e-3}

            timeoutSecs = 500
            # L2 
            kwargs.update({'alpha': 0, 'lambda': 0})
            start = time.time()
            glm = h2o_cmd.runGLM(parseResult=parseResult, 
                timeoutSecs=timeoutSecs, pollTimeoutSecs=60, noise=('JStack', None), **kwargs)
            elapsed = time.time() - start
            print "glm (L2) end on ", csvFilename, 'took', elapsed, 'seconds',\
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
            h2o_glm.simpleCheckGLM(self, glm, None, noPrint=True, **kwargs)
            h2o.check_sandbox_for_errors()

            # Elastic
            kwargs.update({'alpha': 0.5, 'lambda': 1e-4})
            start = time.time()
            glm = h2o_cmd.runGLM(parseResult=parseResult,
                timeoutSecs=timeoutSecs, pollTimeoutSecs=60, noise=('JStack', None), **kwargs)
            elapsed = time.time() - start
            print "glm (Elastic) end on ", csvFilename, 'took', elapsed, 'seconds',\
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
            h2o_glm.simpleCheckGLM(self, glm, None, noPrint=True, **kwargs)
            h2o.check_sandbox_for_errors()

            # L1
            kwargs.update({'alpha': 1.0, 'lambda': 1e-4})
            start = time.time()
            glm = h2o_cmd.runGLM(parseResult=parseResult,
                timeoutSecs=timeoutSecs, pollTimeoutSecs=60, noise=('JStack', None), **kwargs)
            elapsed = time.time() - start
            print "glm (L1) end on ", csvFilename, 'took', elapsed, 'seconds',\
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
            h2o_glm.simpleCheckGLM(self, glm, None, noPrint=True, **kwargs)
            h2o.check_sandbox_for_errors()

            print "Trial #", trial, "completed in", time.time() - trialStart, "seconds.", \


if __name__ == '__main__':
    h2o.unit_main()
