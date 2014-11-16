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

class releaseTest(h2o_common.ReleaseCommon, unittest.TestCase):

    def test_c10_glm_fvec(self):
        print "Since the python is not necessarily run as user=0xcust..., can't use a  schema='put' here"
        print "Want to be able to run python as jenkins"
        print "I guess for big 0xcust files, we don't need schema='put'"
        print "For files that we want to put (for testing put), we can get non-private files"

        # Parse Train***********************************************************
        importFolderPath = '/mnt/0xcustomer-datasets/c3'
        csvFilename = 'classification1Train.txt'
        csvPathname = importFolderPath + "/" + csvFilename

        start = time.time()

        # hack. force it to NA the header, so we have col names that are not customer senstive below
        parseResult = h2i.import_parse(path=csvPathname, schema='local', timeoutSecs=500, doSummary=False, header=0)
        print "Parse of", parseResult['destination_key'], "took", time.time() - start, "seconds"

        print "Parse result['destination_key']:", parseResult['destination_key']

        start = time.time()
        inspect = h2o_cmd.runInspect(None, parseResult['destination_key'], timeoutSecs=500)
        print "Inspect:", parseResult['destination_key'], "took", time.time() - start, "seconds"
        h2o_cmd.infoFromInspect(inspect, csvPathname)
        numRows = inspect['numRows']
        numCols = inspect['numCols']

        # do summary of the parsed dataset last, since we know it fails on this dataset
        summaryResult = h2o_cmd.runSummary(key=parseResult['destination_key'])
        h2o_cmd.infoFromSummary(summaryResult, noPrint=False)

        # keepList = []
        # h2o_glm.findXFromColumnInfo(key=parseResult['destination_key'], keepList=keepList)
        # see README.txt in 0xcustomer-datasets/c3 for the col names to use in keepList above, to get the indices
        
        y = 0
        ignore_x = []
        x = [6,7,8,10,12,31,32,33,34,35,36,37,40,41,42,43,44,45,46,47,49,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70]
        for i in range(numCols):
            if i not in x and i!=y:
                ignore_x.append(i)

        # since we're no long zero based, increment by 1
        ignore_x = ",".join(map(lambda x: "C" + str(x+1), ignore_x))

        
        # GLM Train***********************************************************
        keepPattern = None
        print "y:", y
        # don't need the intermediate Dicts produced from columnInfoFromInspect
        x = h2o_glm.goodXFromColumnInfo(y, keepPattern=keepPattern, key=parseResult['destination_key'], timeoutSecs=300)
        print "x:", x
        print "ignore_x:", x

        kwargs = {
            'response': y,
            'ignored_cols': ignore_x,
            'family': 'binomial',
            'lambda': 1.0E-5,
            'alpha': 0.5,
            'max_iter': 10,
            'n_folds': 1,
            'beta_epsilon': 1.0E-4,
            }

        timeoutSecs = 3600
        start = time.time()
        glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=timeoutSecs, pollTimeoutSecs=60, **kwargs)
        elapsed = time.time() - start
        print "glm completed in", elapsed, "seconds.", \
            "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)

        h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)

        # Parse Test***********************************************************
        GLMModel = glm['glm_model']
        modelKey = GLMModel['_key']

        csvFilename = 'classification1Test.txt'
        csvPathname = importFolderPath + "/" + csvFilename
        parseResult = h2i.import_parse(path=csvPathname, schema='local', timeoutSecs=500, doSummary=False)
        print "Parse of", parseResult['destination_key'], "took", time.time() - start, "seconds"

        # GLMScore Test***********************************************************
        # start = time.time()
        # # score with same dataset (will change to recreated dataset with one less enum
        # glmScore = h2o_cmd.runGLMScore(key=parseResult['destination_key'],
        #     model_key=modelKey, thresholds="0.5", timeoutSecs=timeoutSecs)
        # print "glmScore end on ", parseResult['destination_key'], 'took', time.time() - start, 'seconds'
         

if __name__ == '__main__':
    h2o.unit_main()
