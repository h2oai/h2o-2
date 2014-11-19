import unittest, random, sys, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i, h2o_glm, h2o_util

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init(1,java_heap_GB=14)

    @classmethod
    def tearDownClass(cls):
        # time.sleep(3600)
        h2o.tear_down_cloud()

    def test_short(self):
            csvFilename = 'part-00000b'
            ### csvFilename = 'short'
            print "this data is only on 0xdata machines"
            importFolderPath = '/home/hduser/data'
            csvPathname = importFolderPath + "/" + csvFilename

            # FIX! does 'separator=' take ints or ?? hex format
            # looks like it takes the hex string (two chars)
            start = time.time()
            # hardwire TAB as a separator, as opposed to white space (9)
            parseResult = h2i.import_parse(path=csvPathname, schema='local', timeoutSecs=500, separator=9)
            print "Parse of", parseResult['destination_key'], "took", time.time() - start, "seconds"

            print "Parse result['destination_key']:", parseResult['destination_key']

            start = time.time()
            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'], timeoutSecs=500)
            print "Inspect:", parseResult['destination_key'], "took", time.time() - start, "seconds"
            h2o_cmd.infoFromInspect(inspect, csvPathname)
            # numRows = inspect['numRows']
            # numCols = inspect['numCols']

            keepPattern = "oly_|mt_|b_"
            y = "is_purchase"
            print "y:", y
            # don't need the intermediate Dicts produced from columnInfoFromInspect
            x = h2o_glm.goodXFromColumnInfo(y, keepPattern=keepPattern, key=parseResult['destination_key'], timeoutSecs=300)
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
            glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=timeoutSecs, pollTimeoutSecs=60, **kwargs)
            elapsed = time.time() - start
            print "glm completed in", elapsed, "seconds.", \
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)

            h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)


if __name__ == '__main__':
    h2o.unit_main()
