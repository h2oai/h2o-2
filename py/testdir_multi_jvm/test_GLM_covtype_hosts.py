import os, json, unittest, time, shutil, sys, random
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_glm, h2o_hosts

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global local_host
        local_host = not 'hosts' in os.getcwd()
        if (local_host):
            h2o.build_cloud(3,java_heap_GB=4)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GLM_covtype(self):
        csvFilename = 'covtype.data'
        csvPathname = h2o.find_dataset('UCI/UCI-large/covtype/' + csvFilename)
        parseKey = h2o_cmd.parseFile(csvPathname=csvPathname,timeoutSecs=10)
        inspect = h2o_cmd.runInspect(None, parseKey['destination_key'])
        print "\n" + csvPathname, \
            "    num_rows:", "{:,}".format(inspect['num_rows']), \
            "    num_cols:", "{:,}".format(inspect['num_cols'])

        if (1==0):
            print "WARNING: just doing the first 33 features, for comparison to allstate numbers"
            # pythonic!
            x = ",".join(map(str,range(33)))
        else:
            x = ""

        print "WARNING: max_iter set to 8 for benchmark comparisons"
        max_iter = 8

        y = "54"

        # L2 
        kwargs = {
            'x': x,
            'y': y,
            'family': 'binomial',
            'link': 'logit',
            'num_cross_validation_folds': 0,
            'case_mode': '=',
            'case': 1,
            'max_iter': max_iter,
            'beta_eps': 1e-3}

        timeoutSecs = 120

        start = time.time()
        kwargs.update({'alpha': 0, 'lambda': 0})
        glm = h2o_cmd.runGLMOnly(parseKey=parseKey, timeoutSecs=timeoutSecs, **kwargs)
        print "glm (L2) end on ", csvPathname, 'took', time.time() - start, 'seconds'
        h2o_glm.simpleCheckGLM(self, glm, 13, **kwargs)

        # Elastic
        kwargs.update({'alpha': 0.5, 'lambda': 1e-4})
        start = time.time()
        glm = h2o_cmd.runGLMOnly(parseKey=parseKey, timeoutSecs=timeoutSecs, **kwargs)
        print "glm (Elastic) end on ", csvPathname, 'took', time.time() - start, 'seconds'
        h2o_glm.simpleCheckGLM(self, glm, 13, **kwargs)

        # L1
        kwargs.update({'alpha': 1, 'lambda': 1e-4})
        start = time.time()
        glm = h2o_cmd.runGLMOnly(parseKey=parseKey, timeoutSecs=timeoutSecs, **kwargs)
        print "glm (L1) end on ", csvPathname, 'took', time.time() - start, 'seconds'
        h2o_glm.simpleCheckGLM(self, glm, 13, **kwargs)


if __name__ == '__main__':
    h2o.unit_main()
