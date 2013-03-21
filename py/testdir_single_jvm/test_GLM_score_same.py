
## Dataset created from this:
#
# from sklearn.datasets import make_hastie_10_2
# import numpy as np
# i = 1000000
# f = 10
# (X,y) = make_hastie_10_2(n_samples=i,random_state=None)
# y.shape = (i,1)
# Y = np.hstack((X,y))
# np.savetxt('./1mx' + str(f) + '_hastie_10_2.data', Y, delimiter=',', fmt='%.2f');

import os, json, unittest, time, shutil, sys
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_glm, h2o_util
import copy


def glm_doit(self, csvFilename, csvPathname, timeoutSecs, pollTimeoutSecs, **kwargs):
    print "\nStarting GLM of", csvFilename
    parseKey = h2o_cmd.parseFile(csvPathname=csvPathname, key2=csvFilename + ".hex", 
        timeoutSecs=60, pollTimeoutSecs=pollTimeoutSecs)

    start = time.time()
    glm = h2o_cmd.runGLMOnly(parseKey=parseKey, timeoutSecs=timeoutSecs, **kwargs)
    print "GLM in",  (time.time() - start), "secs (python)"
    h2o_glm.simpleCheckGLM(self, glm, 7, **kwargs)

    # compare this glm to the first one. since the files are replications, the results
    # should be similar?
    GLMModel = glm['GLMModel']
    validationsList = glm['GLMModel']['validations']
    validations = validationsList[0]
    modelKey = GLMModel['model_key']
    return modelKey, validations

def glm_score(self, csvFilename, csvPathname, modelKey, timeoutSecs=30, pollTimeoutSecs=30):
    print "\nStarting GLM score of", csvFilename
    key2 = csvFilename + ".hex"
    parseKey = h2o_cmd.parseFile(csvPathname=csvPathname, key2=key2, 
        timeoutSecs=timeoutSecs, pollTimeoutSecs=pollTimeoutSecs)
    y = "10"
    x = ""
    kwargs = {'x': x, 'y':  y, 'case': -1, 'thresholds': 0.5}

    start = time.time()
    glmScore = h2o_cmd.runGLMScore(key=key2, model_key=modelKey, timeoutSecs=timeoutSecs)
    print "GLMScore in",  (time.time() - start), "secs (python)"
    h2o.verboseprint(h2o.dump_json(glmScore))
    ### h2o_glm.simpleCheckGLM(self, glm, 7, **kwargs)

    # compare this glm to the first one. since the files are replications, 
    # the results
    # should be similar?
    # UPDATE: format for returning results is slightly different than normal GLM
    validation = glmScore['validation']
    if self.validations1:
        h2o_glm.compareToFirstGlm(self, 'err', validation, self.validations1)
    else:
        self.validations1 = copy.deepcopy(validation)



class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()


    @classmethod
    def setUpClass(cls):
        h2o.build_cloud(1)
        global SYNDATASETS_DIR
        SYNDATASETS_DIR = h2o.make_syn_dir()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()


    validations1 = {}

    def test_A_1mx10_hastie_10_2(self):
        # gunzip it and cat it to create 2x and 4x replications in SYNDATASETS_DIR
        csvFilename = "1mx10_hastie_10_2.data.gz"
        csvPathname = h2o.find_dataset('logreg' + '/' + csvFilename)

        y = "10"
        x = ""
        kwargs = {'x': x, 'y':  y, 'case': -1, 'thresholds': 0.5}
        (modelKey, validations1) = glm_doit(self, csvFilename, csvPathname, 
            timeoutSecs=60, pollTimeoutSecs=60, **kwargs)

        print "Use", modelKey, "model on 2x and 4x replications and compare results to 1x"

        filename1x = "hastie_1x.data"
        pathname1x = SYNDATASETS_DIR + '/' + filename1x
        h2o_util.file_gunzip(csvPathname, pathname1x)

        filename2x = "hastie_2x.data"
        pathname2x = SYNDATASETS_DIR + '/' + filename2x
        h2o_util.file_cat(pathname1x,pathname1x,pathname2x)
        glm_score(self,filename2x, pathname2x, modelKey, 
            timeoutSecs=60, pollTimeoutSecs=60)

        filename4x = "hastie_4x.data"
        pathname4x = SYNDATASETS_DIR + '/' + filename4x
        h2o_util.file_cat(pathname2x,pathname2x,pathname4x)
        
        print "Iterating 3 times on this last one"
        for i in range(3):
            print "\nTrial #", i, "of", filename4x
            glm_score(self,filename4x, pathname4x, modelKey, 
                timeoutSecs=60, pollTimeoutSecs=60)

if __name__ == '__main__':
    h2o.unit_main()
