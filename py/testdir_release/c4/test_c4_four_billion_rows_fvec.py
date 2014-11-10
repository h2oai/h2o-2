import unittest, time, sys, random
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_glm, h2o_browse as h2b, h2o_import as h2i, h2o_common, h2o_exec as h2e

print "Assumes you ran ../build_for_clone.py in this directory"
print "Using h2o-nodes.json. Also the sandbox dir"

class releaseTest(h2o_common.ReleaseCommon, unittest.TestCase):

    def test_four_billion_rows_fvec(self):
        timeoutSecs = 1500

        importFolderPath = "billions"
        csvFilenameList = [
            "four_billion_rows.csv",
            ]
        for csvFilename in csvFilenameList:
            csvPathname = importFolderPath + "/" + csvFilename
            start = time.time()

            # Parse*********************************
            parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, schema='local',
                timeoutSecs=timeoutSecs, pollTimeoutSecs=180, retryDelaySecs=3)
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
                "    numCols:", "{:,}".format(numCols), \
                "    byteSize:", "{:,}".format(byteSize)

            expectedRowSize = numCols * 1 # plus output
            # expectedValueSize = expectedRowSize * numRows
            expectedValueSize =  8001271520
            self.assertEqual(byteSize, expectedValueSize,
                msg='byteSize %s is not expected: %s' % \
                (byteSize, expectedValueSize))

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
                'max_iter': 10,
                'normalize': 0,
                'destination_key': 'junk.hex',
                'seed': 265211114317615310,
                }

            timeoutSecs = 900
            start = time.time()
            kmeans = h2o_cmd.runKMeans(parseResult=parseResult, timeoutSecs=timeoutSecs, retryDelaySecs=4, **kwargs)

            # GLM*********************************
            print "\n" + csvFilename
            kwargs = {
                'response': 'C1',
                'n_folds': 0, 
                'family': 'binomial',
            }
            # one coefficient is checked a little more
            colX = 1

            # convert to binomial
            execExpr="A.hex=%s" % parseResult['destination_key']
            h2e.exec_expr(execExpr=execExpr, timeoutSecs=180)
            execExpr="A.hex[,%s]=(A.hex[,%s]==%s)" % ('1', '1', 1)
            h2e.exec_expr(execExpr=execExpr, timeoutSecs=180)
            aHack = {'destination_key': "A.hex"}

            # L2 
            timeoutSecs = 900
            kwargs.update({'alpha': 0, 'lambda': 0})
            start = time.time()
            glm = h2o_cmd.runGLM(parseResult=aHack, timeoutSecs=timeoutSecs, **kwargs)
            elapsed = time.time() - start
            print "glm (L2) end on ", csvFilename, 'took', elapsed, 'seconds.', "%d pct. of timeout" % ((elapsed/timeoutSecs) * 100)
            h2o_glm.simpleCheckGLM(self, glm, "C" + str(colX), **kwargs)

if __name__ == '__main__':
    h2o.unit_main()
