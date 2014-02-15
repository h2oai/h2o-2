import unittest, sys, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_import as h2i, h2o_common, h2o_print, h2o_glm

print "Assumes you ran ../../cloud.py in this directory"
print "Using h2o-nodes.json. Also the sandbox dir"

print "Uses 0xcust.. data. Cloud must be built as 0xcust.. and run with access to 0xdata /mnt automounts"
print "Assume there are links in /home/0xcust.. to the nas bucket used"
print "i.e. /home/0xcust... should have results of 'ln -s /mnt/0xcustomer-datasets"
print "The path resolver in python tests will find it in the home dir of the username being used"
print "to run h2o..i.e from the config json which builds the cloud and passes that info to the test"
print "via the cloned cloud mechanism (h2o-nodes.json)"


DO_GLM = False
class releaseTest(h2o_common.ReleaseCommon, unittest.TestCase):

    def test_c7_rel(self):
        h2o.beta_features = False
        print "Since the python is not necessarily run as user=0xcust..., can't use a  schema='put' here"
        print "Want to be able to run python as jenkins"
        print "I guess for big 0xcust files, we don't need schema='put'"
        print "For files that we want to put (for testing put), we can get non-private files"

        csvFilename = 'part-00000b'
        importFolderPath = '/mnt/0xcustomer-datasets/c2'
        csvPathname = importFolderPath + "/" + csvFilename

        # FIX! does 'separator=' take ints or ?? hex format
        # looks like it takes the hex string (two chars)
        start = time.time()
        # hardwire TAB as a separator, as opposed to white space (9)
        parseResult = h2i.import_parse(path=csvPathname, schema='local', timeoutSecs=500, separator=9, doSummary=False)
        print "Parse of", parseResult['destination_key'], "took", time.time() - start, "seconds"

        start = time.time()
        inspect = h2o_cmd.runInspect(None, parseResult['destination_key'], timeoutSecs=500)
        print "Inspect:", parseResult['destination_key'], "took", time.time() - start, "seconds"
        h2o_cmd.infoFromInspect(inspect, csvPathname)
        num_rows = inspect['num_rows']
        num_cols = inspect['num_cols']
        print "\n" + csvFilename, "    num_rows:", "{:,}".format(num_rows), "    num_cols:", "{:,}".format(num_cols)

        summaryResult = h2o_cmd.runSummary(key=parseResult['destination_key'], numCols=num_cols, numRows=num_rows, max_column_display=2500)
        # it's in runSummary!
        # h2o_cmd.infoFromSummary(summaryResult, noPrint=False, numCols=num_cols, numRows=num_rows)

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
            'max_iter': 4,
            'n_folds': 1,
            'beta_epsilon': 1.0E-4,
            }

        timeoutSecs = 3600

        if DO_GLM:
            start = time.time()
            glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=timeoutSecs, pollTimeoutSecs=60, **kwargs)
            elapsed = time.time() - start
            print "glm completed in", elapsed, "seconds.", \
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
            h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)


if __name__ == '__main__':
    h2o.unit_main()
