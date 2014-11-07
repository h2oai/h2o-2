import unittest, time, sys, random
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_glm, h2o_browse as h2b, h2o_import as h2i, h2o_exec as h2e

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init(1,java_heap_GB=14)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_four_billion_rows(self):
        timeoutSecs = 1500

        importFolderPath = "billions"
        csvFilenameList = [
            ("four_billion_rows.csv", "a.hex"),
            ("four_billion_rows.csv", "b.hex"),
            ]
        for (csvFilename, hex_key) in csvFilenameList:
            csvPathname = importFolderPath + "/" + csvFilename
            start = time.time()

            # Parse*********************************
            parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, schema='local', hex_key=hex_key,
                timeoutSecs=timeoutSecs, pollTimeoutSecs=60)
            elapsed = time.time() - start
            print "Parse result['destination_key']:", parseResult['destination_key']
            print csvFilename, "completed in", elapsed, "seconds.", "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)

            # Inspect*********************************
            # We should be able to see the parse result?
            inspect = h2o_cmd.runInspect(key=parseResult['destination_key'])
            numCols = inspect['numCols']
            numRows = inspect['numRows']
            byteSize = inspect['byteSize']
            print "\n" + csvFilename, \
                "    numRows:", "{:,}".format(numRows), \
                "    numCols:", "{:,}".format(numCols)

            summaryResult = h2o_cmd.runSummary(key=parseResult['destination_key'], timeoutSecs=timeoutSecs)
            h2o_cmd.infoFromSummary(summaryResult, noPrint=True)

            self.assertEqual(2, numCols,
                msg="generated %s cols (including output).  parsed to %s cols" % (2, numCols))
            self.assertEqual(4*1000000000, numRows,
                msg="generated %s rows, parsed to %s rows" % (4*1000000000, numRows))

            # KMeans*********************************
            kwargs = {
                'k': 3,
                'initialization': 'Furthest',
                'max_iter': 20,
                'normalize': 0,
                'destination_key': 'junk.hex',
                'seed': 265211114317615310,
                }

            timeoutSecs = 900
            start = time.time()
            kmeans = h2o_cmd.runKMeans(parseResult=parseResult, timeoutSecs=timeoutSecs, **kwargs)

            # Exec to make binomial########################
            execExpr="%s[,%s]=(%s[,%s]==%s)" % (hex_key, 1+1, hex_key, 1+1, 0)
            h2e.exec_expr(execExpr=execExpr, timeoutSecs=30)

            # GLM*********************************
            print "\n" + csvFilename
            colX = 0
            kwargs = {
                'response': 'C2', 
                'n_folds': 0,
                'cols': colX, 
                'alpha': 0, 
                'lambda': 0, 
                'family': 'binomial',
                # 'link' can be family_default, identity, logit, log, inverse, tweedie
            }
            # one coefficient is checked a little more

            # L2 
            timeoutSecs = 900
            start = time.time()
            glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=timeoutSecs, **kwargs)
            elapsed = time.time() - start
            print "glm (L2) end on ", csvFilename, 'took', elapsed, 'seconds.', "%d pct. of timeout" % ((elapsed/timeoutSecs) * 100)
            h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)

if __name__ == '__main__':
    h2o.unit_main()
