import unittest, time, sys, random
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_glm, h2o_hosts, h2o_import as h2i

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(3,java_heap_GB=4)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GLM_covtype(self):
        csvFilename = 'covtype.data'
        csvPathname = 'standard/' + csvFilename
        hex_key = 'covtype.hex'
        parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, schema='put',
            hex_key=hex_key, timeoutSecs=10)

        inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
        print "\n" + csvPathname, \
            "    num_rows:", "{:,}".format(inspect['num_rows']), \
            "    num_cols:", "{:,}".format(inspect['num_cols'])

        print "WARNING: max_iter set to 8 for benchmark comparisons"
        max_iter = 8

        y = "54"
        x = ""

        print "Touching it with exec to trigger va to fvec (covtype.hex) , and then fvec to va (covtype2.hex)"
        h2o_cmd.runExec(str='%s=%s' % ('covtype2.hex', hex_key))
        # hack to use the new one
        parseResult['destination_key'] = 'covtype2.hex'

        # L2 
        kwargs = {
            'x': x,
            'y': y,
            'family': 'binomial',
            'link': 'logit',
            'n_folds': 0,
            'case_mode': '=',
            'case': 1,
            'max_iter': max_iter,
            'beta_epsilon': 1e-3}

        timeoutSecs = 120

        start = time.time()
        kwargs.update({'alpha': 0, 'lambda': 0})
        glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=timeoutSecs, **kwargs)
        print "glm (L2) end on ", csvPathname, 'took', time.time() - start, 'seconds'
        h2o_glm.simpleCheckGLM(self, glm, 13, **kwargs)

        # Elastic
        kwargs.update({'alpha': 0.5, 'lambda': 1e-4})
        start = time.time()
        glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=timeoutSecs, **kwargs)
        print "glm (Elastic) end on ", csvPathname, 'took', time.time() - start, 'seconds'
        h2o_glm.simpleCheckGLM(self, glm, 13, **kwargs)

        # L1
        kwargs.update({'alpha': 1, 'lambda': 1e-4})
        start = time.time()
        glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=timeoutSecs, **kwargs)
        print "glm (L1) end on ", csvPathname, 'took', time.time() - start, 'seconds'
        h2o_glm.simpleCheckGLM(self, glm, 13, **kwargs)


if __name__ == '__main__':
    h2o.unit_main()
