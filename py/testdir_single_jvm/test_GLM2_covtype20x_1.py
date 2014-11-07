import unittest, time, sys, random
sys.path.extend(['.','..','../..','py'])

import h2o, h2o_cmd, h2o_glm, h2o_exec as h2e
import h2o_browse as h2b, h2o_import as h2i


class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init(1,java_heap_GB=14)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GLM2_covtype20x_1(self):
        csvFilenameList = [
            ('covtype20x.data', 800),
            ]

        # a browser window too, just because we can
        # h2b.browseTheCloud()

        importFolderPath = 'standard'
        for csvFilename, timeoutSecs in csvFilenameList:
            csvPathname = importFolderPath + "/" + csvFilename
            hex_key = "A.hex"
            parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, schema='put',
                hex_key = hex_key, timeoutSecs=2000, pollTimeoutSecs=60)
            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
            print "\n" + csvPathname, \
                "    numRows:", "{:,}".format(inspect['numRows']), \
                "    numCols:", "{:,}".format(inspect['numCols'])

            print "WARNING: max_iter set to 8 for benchmark comparisons"
            max_iter = 8

            y = 54
            kwargs = {
                'response': 'C' + str(y+1), # for 2
                'family': 'binomial',
                'n_folds': 2,
                'max_iter': max_iter,
                'beta_epsilon': 1e-3,
                # 'destination_key': modelKey
                }

            execExpr="A.hex[,%s]=(A.hex[,%s]>%s)" % (y+1, y+1, 1)
            h2e.exec_expr(execExpr=execExpr, timeoutSecs=30)
            aHack = {'destination_key': 'A.hex'}


            # L2 
            kwargs.update({'alpha': 0, 'lambda': 0})
            start = time.time()
            glm = h2o_cmd.runGLM(parseResult=aHack, timeoutSecs=timeoutSecs, **kwargs)
            elapsed = time.time() - start
            print "glm (L2) end on ", csvPathname, 'took', elapsed, 'seconds.', "%d pct. of timeout" % ((elapsed/timeoutSecs) * 100)
            h2o_glm.simpleCheckGLM(self, glm, "C14", **kwargs)

            # Elastic
            kwargs.update({'alpha': 0.5, 'lambda': 1e-4})
            start = time.time()
            glm = h2o_cmd.runGLM(parseResult=aHack, timeoutSecs=timeoutSecs, **kwargs)
            elapsed = time.time() - start
            print "glm (Elastic) end on ", csvPathname, 'took', elapsed, 'seconds.', "%d pct. of timeout" % ((elapsed/timeoutSecs) * 100)
            h2o_glm.simpleCheckGLM(self, glm, "C14", **kwargs)

            # L1
            kwargs.update({'alpha': 1.0, 'lambda': 1e-4})
            start = time.time()
            glm = h2o_cmd.runGLM(parseResult=aHack, timeoutSecs=timeoutSecs, **kwargs)
            elapsed = time.time() - start
            print "glm (L1) end on ", csvPathname, 'took', elapsed, 'seconds.', "%d pct. of timeout" % ((elapsed/timeoutSecs) * 100)
            h2o_glm.simpleCheckGLM(self, glm, "C14", **kwargs)


if __name__ == '__main__':
    h2o.unit_main()
