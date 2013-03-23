import os, json, unittest, time, shutil, sys, random
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
            # None is okay for key2
            csvFilenameList = [
                ('covtype20x.data', 480,'cA'),
                # ('covtype200x.data', 1000,'cE'),
                ]

        # a browser window too, just because we can
        h2b.browseTheCloud()

        importFolderPath = '/home/0xdiag/datasets'
        h2i.setupImportFolder(None, importFolderPath)
        for csvFilename, timeoutSecs, key2 in csvFilenameList:
            csvPathname = importFolderPath + "/" + csvFilename
            # creates csvFilename.hex from file in importFolder dir 
            start = time.time()
            parseKey = h2i.parseImportFolderFile(None, csvFilename, importFolderPath, 
                timeoutSecs=2000, key2=key2)
            print "parse end on ", csvPathname, 'took', time.time() - start, 'seconds'
            h2o.check_sandbox_for_errors()

            inspect = h2o_cmd.runInspect(None, parseKey['destination_key'])
            print "\n" + csvPathname, \
                "    num_rows:", "{:,}".format(inspect['num_rows']), \
                "    num_cols:", "{:,}".format(inspect['num_cols'])

            if (1==0):
                print "WARNING: just first 33 features. Comparison to allstate"
                # pythonic!
                x = ",".join(map(str,range(33)))
            else:
                x = ""

            print "WARNING: max_iter set to 8 for benchmark comparisons"
            max_iter = 50

            y = "54"

            kwargs = {
                'x': x,
                'y': y, 
                'family': 'binomial',
                'link': 'logit',
                'num_cross_validation_folds': 0, 
                'case_mode': '=', 
                'case': 1, 
                'max_iter': max_iter, 
                'beta_epsilon': 1e-3}

            # L2 
            kwargs.update({'alpha': 0, 'lambda': 0})
            start = time.time()
            glm = h2o_cmd.runGLMOnly(parseKey=parseKey, timeoutSecs=timeoutSecs, **kwargs)
            print "glm (L2) end on ", csvPathname, 'took', time.time() - start, 'seconds'
            h2o_glm.simpleCheckGLM(self, glm, 13, **kwargs)
            h2o.check_sandbox_for_errors()

            # Elastic
            kwargs.update({'alpha': 0.5, 'lambda': 1e-4})
            start = time.time()
            glm = h2o_cmd.runGLMOnly(parseKey=parseKey, timeoutSecs=timeoutSecs, **kwargs)
            print "glm (Elastic) end on ", csvPathname, 'took', time.time() - start, 'seconds'
            h2o_glm.simpleCheckGLM(self, glm, 13, **kwargs)
            h2o.check_sandbox_for_errors()

            # L1
            kwargs.update({'alpha': 1.0, 'lambda': 1e-4})
            start = time.time()
            glm = h2o_cmd.runGLMOnly(parseKey=parseKey, timeoutSecs=timeoutSecs, **kwargs)
            print "glm (L1) end on ", csvPathname, 'took', time.time() - start, 'seconds'
            h2o_glm.simpleCheckGLM(self, glm, 13, **kwargs)
            h2o.check_sandbox_for_errors()



if __name__ == '__main__':
    h2o.unit_main()
