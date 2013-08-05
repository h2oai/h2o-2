import unittest
import random, sys, time, re
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import as h2i, h2o_glm, h2o_util
class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED, localhost
        SEED = h2o.setup_random_seed()
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(1,java_heap_GB=28)
        else:
            h2o_hosts.build_cloud_with_hosts(java_heap_GB=28)

    @classmethod
    def tearDownClass(cls):
        # time.sleep(3600)
        h2o.tear_down_cloud()

    def test_short(self):
            csvFilename = 'part-00000b'
            ### csvFilename = 'short'
            importFolderPath = '/home/hduser/data'
            importFolderResult = h2i.setupImportFolder(None, importFolderPath)
            csvPathname = importFolderPath + "/" + csvFilename

            # FIX! does 'separator=' take ints or ?? hex format
            # looks like it takes the hex string (two chars)
            start = time.time()
            # hardwire TAB as a separator, as opposed to white space (9)
            parseKey = h2i.parseImportFolderFile(None, csvFilename, importFolderPath, 
                timeoutSecs=500, separator=9)
            print "Parse of", parseKey['destination_key'], "took", time.time() - start, "seconds"

            print csvFilename, 'parse time:', parseKey['response']['time']
            print "Parse result['destination_key']:", parseKey['destination_key']

            start = time.time()
            inspect = h2o_cmd.runInspect(None, parseKey['destination_key'], timeoutSecs=500)
            print "Inspect:", parseKey['destination_key'], "took", time.time() - start, "seconds"
            h2o_cmd.infoFromInspect(inspect, csvPathname)
            # num_rows = inspect['num_rows']
            # num_cols = inspect['num_cols']

            keepPattern = "oly_|mt_|b_"
            y = "is_purchase"
            print "y:", y
            # don't need the intermediate Dicts produced from columnInfoFromInspect
            x = h2o_glm.goodXFromColumnInfo(y, keepPattern=keepPattern, key=parseKey['destination_key'], timeoutSecs=300)
            print "x:", x

            kwargs = {
                'x': x, 
                'y': y,
                # 'case_mode': '>',
                # 'case': 0,
                'family': 'binomial',
                'lambda': 1.0E-5,
                'alpha': 0.5,
                'max_iter': 5,
                'thresholds': 0.5,
                'n_folds': 1,
                'weight': 100,
                'beta_epsilon': 1.0E-4,
                }

            timeoutSecs = 1800
            start = time.time()
            glm = h2o_cmd.runGLMOnly(parseKey=parseKey, timeoutSecs=timeoutSecs, pollTimeoutsecs=60, **kwargs)
            elapsed = time.time() - start
            print "glm completed in", elapsed, "seconds.", \
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)

            h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)


if __name__ == '__main__':
    h2o.unit_main()
