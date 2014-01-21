import unittest, time, sys, random
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_glm, h2o_hosts
import h2o_browse as h2b, h2o_import as h2i

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(2,java_heap_GB=5)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GLM_covtype20x(self):
        if localhost:
            csvFilenameList = [
                # 68 secs on my laptop?
                ('covtype20x.data', 480, 'cA'),
                ]
        else:
            # None is okay for hex_key
            csvFilenameList = [
                ('covtype20x.data', 480,'cA'),
                # ('covtype200x.data', 1000,'cE'),
                ]

        # a browser window too, just because we can
        ### h2b.browseTheCloud()
        importFolderPath = "standard"
        for csvFilename, timeoutSecs, hex_key in csvFilenameList:
            csvPathname = importFolderPath + "/" + csvFilename
            start = time.time()
            parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, 
                timeoutSecs=2000, hex_key=hex_key)
            print "parse end on ", csvPathname, 'took', time.time() - start, 'seconds'
            h2o.check_sandbox_for_errors()

            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
            print "\n" + csvPathname, \
                "    num_rows:", "{:,}".format(inspect['num_rows']), \
                "    num_cols:", "{:,}".format(inspect['num_cols'])


            # this will make it fvec
            print "Touching %s with exec to make it fvec" % hex_key
            h2o_cmd.runExec(str='%s[0,]=%s[0,]' % (hex_key, hex_key))
            print "WARNING: max_iter set to 8 for benchmark comparisons"
            max_iter = 8 

            y = "54"
            x = ""

            kwargs = {
                'x': x,
                'y': y, 
                'family': 'binomial',
                'link': 'logit',
                'n_folds': 1, 
                'case_mode': '=', 
                'case': 1, 
                'max_iter': max_iter, 
                'beta_epsilon': 1e-3}

            # L2 
            kwargs.update({'alpha': 0, 'lambda': 0})
            start = time.time()
            glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=timeoutSecs, noise=('JStack', None), **kwargs)
            print "glm (L2) end on ", csvPathname, 'took', time.time() - start, 'seconds'
            h2o_glm.simpleCheckGLM(self, glm, 13, **kwargs)
            h2o.check_sandbox_for_errors()

            # Elastic
            kwargs.update({'alpha': 0.5, 'lambda': 1e-4})
            start = time.time()
            glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=timeoutSecs, noise=('JStack', None), **kwargs)
            print "glm (Elastic) end on ", csvPathname, 'took', time.time() - start, 'seconds'
            h2o_glm.simpleCheckGLM(self, glm, 13, **kwargs)
            h2o.check_sandbox_for_errors()

            # L1
            kwargs.update({'alpha': 1.0, 'lambda': 1e-4})
            start = time.time()
            glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=timeoutSecs, noise=('JStack', None), **kwargs)
            print "glm (L1) end on ", csvPathname, 'took', time.time() - start, 'seconds'
            h2o_glm.simpleCheckGLM(self, glm, 13, **kwargs)
            h2o.check_sandbox_for_errors()



if __name__ == '__main__':
    h2o.unit_main()
