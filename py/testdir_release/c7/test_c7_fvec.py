import unittest, sys, time, getpass
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_import as h2i, h2o_common, h2o_print, h2o_glm, h2o_jobs as h2j

print "Assumes you ran ../../cloud.py in this directory"
print "Using h2o-nodes.json. Also the sandbox dir"

print "Uses 0xcust.. data. Cloud must be built as 0xcust.. and run with access to 0xdata /mnt automounts"
print "Assume there are links in /home/0xcust.. to the nas bucket used"
print "i.e. /home/0xcust... should have results of 'ln -s /mnt/0xcustomer-datasets"
print "The path resolver in python tests will find it in the home dir of the username being used"
print "to run h2o..i.e from the config json which builds the cloud and passes that info to the test"
print "via the cloned cloud mechanism (h2o-nodes.json)"

class releaseTest(h2o_common.ReleaseCommon, unittest.TestCase):

    def test_c7_fvec(self):
        print "Since the python is not necessarily run as user=0xcust.."
        print "r can't use schema='put' here"
        print "Want to be able to run python as jenkins"
        print "I guess for big 0xcust files, we don't need schema='put'"
        print "For files that we want to put (for testing put), we can get non-private files"

        # apparently h2o will create a "_" to replace the "-"..so lets force the destination key name
        csvFilename = "part-00000b"
        hex_key = "part_00000b.hex"
        importFolderPath = '/mnt/0xcustomer-datasets/c2'
        csvPathname = importFolderPath + "/" + csvFilename

        # FIX! does 'separator=' take ints or ?? hex format
        # looks like it takes the hex string (two chars)
        start = time.time()
        # hardwire TAB as a separator, as opposed to white space (9)
        parseResult = h2i.import_parse(path=csvPathname, schema='local', 
            separator=9, hex_key=hex_key, doSummary=False, timeoutSecs=500)

        print "Parse of", parseResult['destination_key'], "took", time.time() - start, "seconds"
        print "Parse result['destination_key']:", parseResult['destination_key']

        inspect = h2o_cmd.runInspect(None, hex_key, timeoutSecs=500)
        h2o_cmd.infoFromInspect(inspect, csvPathname)
        numRows = inspect['numRows']
        numCols = inspect['numCols']

        # do summary of the parsed dataset last, since we know it fails on this dataset
        # does the json fail with too many??
        #summaryResult = h2o_cmd.runSummary(key=parseResult['destination_key'], max_ncols=2)
        # summaryResult = h2o_cmd.runSummary(key=parseResult['destination_key'], max_ncols=2500)
        # can't do more than 1000
        summaryResult = h2o_cmd.runSummary(key=hex_key, numCols=numCols, numRows=numRows, timeoutSecs=500)

        # there may be a lot NAs. 
        # we don't want to ignore any cols, and we don't want to ignore row
        # so impute to median
        
        # zero indexed column
        for column in range(numCols):
            print "Imputing any NAs in column %s to median" % column
            impResult = h2o.nodes[0].impute(source=hex_key, column=column, method='median')

        # check that there are no missing now
        inspect = h2o_cmd.runInspect(key=hex_key)
        missingValuesList = h2o_cmd.infoFromInspect(inspect)
        if len(missingValuesList)!=0:
            raise Exception ("Shouldn't be missing values after impute: %s" % missingValuesList)

        keepPattern = "oly_|mt_|b_"
        y = "is_purchase"
        print "y:", y
        # don't need the intermediate Dicts produced from columnInfoFromInspect
        x = h2o_glm.goodXFromColumnInfo(y, keepPattern=keepPattern, key=hex_key)
        print "x:", x

        kwargs = {
            'response': y,
            'family': 'binomial',
            'lambda': 1.0E-5,
            'alpha': 0.5,
            'max_iter': 10,
            # 'thresholds': 0.5,
            'n_folds': 1,
            'beta_epsilon': 1.0E-4,
            }

        timeoutSecs = 3600
        start = time.time()
        glm = h2o_cmd.runGLM(parseResult=parseResult, 
            timeoutSecs=timeoutSecs, pollTimeoutSecs=60, noPoll=True, **kwargs)
        statMean = h2j.pollStatsWhileBusy(timeoutSecs=timeoutSecs, pollTimeoutSecs=30, retryDelaySecs=5)
        num_cpus = statMean['num_cpus'],
        my_cpu_pct = statMean['my_cpu_%'],
        sys_cpu_pct = statMean['sys_cpu_%'],
        system_load = statMean['system_load']
        # shouldn't need this?
        h2j.pollWaitJobs(pattern=None, timeoutSecs=timeoutSecs, pollTimeoutSecs=30, retryDelaySecs=5)

        # can't figure out how I'm supposed to get the model
        # GLMModel = glm['GLMModel']
        # modelKey = GLMModel['model_key']
        # glmView = h2o.nodes[0].glm_view(modelKey=modelKey)

        elapsed = time.time() - start
        print "glm completed in", elapsed, "seconds.", \
            "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)

        # h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)


if __name__ == '__main__':
    h2o.unit_main()
