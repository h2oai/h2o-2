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
            h2o.build_cloud(1,java_heap_GB=4)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_poisson_covtype20x(self):
        if localhost:
            csvFilenameList = [
                ('covtype20x.data', 400),
                ]
        else:
            csvFilenameList = [
                ('covtype20x.data', 400),
                ('covtype200x.data', 2000),
                ]

        # a browser window too, just because we can
        h2b.browseTheCloud()

        importFolderPath = '/home/0xdiag/datasets'
        h2i.setupImportFolder(None, importFolderPath)
        for csvFilename, timeoutSecs in csvFilenameList:
            # creates csvFilename.hex from file in importFolder dir 
            parseKey = h2i.parseImportFolderFile(None, csvFilename, importFolderPath, 
                timeoutSecs=2000, pollTimeoutSecs=60)
            inspect = h2o_cmd.runInspect(None, parseKey['destination_key'])
            csvPathname = importFolderPath + "/" + csvFilename
            print "\n" + csvPathname, \
                "    num_rows:", "{:,}".format(inspect['num_rows']), \
                "    num_cols:", "{:,}".format(inspect['num_cols'])

            if (1==0):
                print "WARNING: just doing the first 33 features, for comparison to ??? numbers"
                # pythonic!
                x = ",".join(map(str,range(33)))
            else:
                x = ""

            print "WARNING: max_iter set to 8 for benchmark comparisons"
            max_iter = 8

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
            elapsed = time.time() - start
            print "glm (L2) end on ", csvPathname, 'took', elapsed, 'seconds.', "%d pct. of timeout" % ((elapsed/timeoutSecs) * 100)
            h2o_glm.simpleCheckGLM(self, glm, 13, **kwargs)

            # Elastic
            kwargs.update({'alpha': 0.5, 'lambda': 1e-4})
            start = time.time()
            glm = h2o_cmd.runGLMOnly(parseKey=parseKey, timeoutSecs=timeoutSecs, **kwargs)
            elapsed = time.time() - start
            print "glm (Elastic) end on ", csvPathname, 'took', elapsed, 'seconds.', "%d pct. of timeout" % ((elapsed/timeoutSecs) * 100)
            h2o_glm.simpleCheckGLM(self, glm, 13, **kwargs)

            # L1
            kwargs.update({'alpha': 1.0, 'lambda': 1e-4})
            start = time.time()
            glm = h2o_cmd.runGLMOnly(parseKey=parseKey, timeoutSecs=timeoutSecs, **kwargs)
            elapsed = time.time() - start
            print "glm (L1) end on ", csvPathname, 'took', elapsed, 'seconds.', "%d pct. of timeout" % ((elapsed/timeoutSecs) * 100)
            h2o_glm.simpleCheckGLM(self, glm, 13, **kwargs)


if __name__ == '__main__':
    h2o.unit_main()
