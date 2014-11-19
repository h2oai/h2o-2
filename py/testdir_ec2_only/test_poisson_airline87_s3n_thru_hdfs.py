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

    def test_poisson_alirline87_s3n_thru_hdfs(self):
        bucket = 'h2o-airlines-unpacked'
        csvFilename = "year1987.csv"
        hex_key = "year1987.hex"
        csvPathname = csvFilename
        trialMax = 2
        timeoutSecs = 500
        for trial in range(trialMax):
            trialStart = time.time()
            hex_key = csvFilename + "_" + str(trial) + ".hex"
            start = time.time()
            parseResult = h2i.import_parse(bucket=bucket, path=csvPathname, schema='s3n', hex_key=hex_key,
                timeoutSecs=timeoutSecs, retryDelaySecs=10, pollTimeoutSecs=1200)
            elapsed = time.time() - start
            print "parse end on ", hex_key, 'took', elapsed, 'seconds',\
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
            print "parse result:", parseResult['destination_key']

            kwargs = {
                # will fail if categorical is chosen
                # 'y': 'IsArrDelayed',
                'y': 'CRSArrTime',
                'x': '1,2,3,4,8,9,16,17,18,30',
                'family': 'poisson',
                'link': 'familyDefault',
                'n_folds': 1,
                'max_iter': 8,
                'beta_epsilon': 1e-3
                }

            timeoutSecs = 500
            # L2 
            kwargs.update({'alpha': 0, 'lambda': 0})
            start = time.time()
            glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=timeoutSecs, pollTimeoutSecs=120, **kwargs)
            elapsed = time.time() - start
            print "glm (L2) end on ", csvPathname, 'took', elapsed, 'seconds',\
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
            h2o_glm.simpleCheckGLM(self, glm, None, noPrint=True, **kwargs)
            h2o.check_sandbox_for_errors()

            # Elastic
            kwargs.update({'alpha': 0.5, 'lambda': 1e-4})
            start = time.time()
            glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=timeoutSecs, pollTimeoutSecs=60, **kwargs)
            elapsed = time.time() - start
            print "glm (Elastic) end on ", csvPathname, 'took', elapsed, 'seconds',\
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
            h2o_glm.simpleCheckGLM(self, glm, None, noPrint=True, **kwargs)
            h2o.check_sandbox_for_errors()

            # L1
            kwargs.update({'alpha': 1.0, 'lambda': 1e-4})
            start = time.time()
            glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=timeoutSecs, pollTimeoutSecs=60, **kwargs)
            elapsed = time.time() - start
            print "glm (L1) end on ", csvPathname, 'took', elapsed, 'seconds',\
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
            h2o_glm.simpleCheckGLM(self, glm, None, noPrint=True, **kwargs)
            h2o.check_sandbox_for_errors()

            print "Trial #", trial, "completed in", time.time() - trialStart, "seconds.", \


if __name__ == '__main__':
    h2o.unit_main()
