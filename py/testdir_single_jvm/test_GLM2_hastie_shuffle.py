# Dataset created from this:
# Elements of Statistical Learning 2nd Ed.; Hastie, Tibshirani, Friedman; Feb 2011
# example 10.2 page 357
# Ten features, standard independent Gaussian. Target y is:
#   y[i] = 1 if sum(X[i]) > .34 else -1
# 9.34 is the median of a chi-squared random variable with 10 degrees of freedom 
# (sum of squares of 10 standard Gaussians)
# http://www.stanford.edu/~hastie/local.ftp/Springer/ESLII_print5.pdf

# from sklearn.datasets import make_hastie_10_2
# import numpy as np
# i = 1000000
# f = 10
# (X,y) = make_hastie_10_2(n_samples=i,random_state=None)
# y.shape = (i,1)
# Y = np.hstack((X,y))
# np.savetxt('./1mx' + str(f) + '_hastie_10_2.data', Y, delimiter=',', fmt='%.2f');

import unittest, time, sys, copy
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_glm, h2o_util, h2o_import as h2i

def glm_doit(self, csvFilename, bucket, csvPathname, timeoutSecs=30):
    print "\nStarting GLM of", csvFilename
    parseResult = h2i.import_parse(bucket=bucket, path=csvPathname, 
        hex_key=csvFilename + ".hex", schema='put', timeoutSecs=30)
    y = 10
    # Took n_folds out, because GLM doesn't include n_folds time and it's slow
    # wanted to compare GLM time to my measured time
    # hastie has two values, 1 and -1. need to use case for one of them
    kwargs = {'response':  y, 'alpha': 0, 'family': 'binomial'}

    h2o.nodes[0].to_enum(src_key=parseResult['destination_key'], column_index=y+1)

    start = time.time()
    glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=timeoutSecs, **kwargs)
    print "GLM in",  (time.time() - start), "secs (python measured)"
    h2o_glm.simpleCheckGLM(self, glm, "C8", **kwargs)

    # compare this glm to the first one. since the files are replications, the results
    # should be similar?
    glm_model = glm['glm_model']
    validation = glm_model['submodels'][0]['validation']

    if self.validation1:
        h2o_glm.compareToFirstGlm(self, 'auc', validation, self.validation1)
    else:
        self.validation1 = copy.deepcopy(validation)

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init(1)
        global SYNDATASETS_DIR
        SYNDATASETS_DIR = h2o.make_syn_dir()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    validation1 = {}
    def test_GLM2_hastie_shuffle(self):
        # gunzip it and cat it to create 2x and 4x replications in SYNDATASETS_DIR
        # FIX! eventually we'll compare the 1x, 2x and 4x results like we do
        # in other tests. (catdata?)

        # This test also adds file shuffling, to see that row order doesn't matter
        csvFilename = "1mx10_hastie_10_2.data.gz"
        bucket = 'home-0xdiag-datasets'
        csvPathname = 'standard' + '/' + csvFilename
        fullPathname = h2i.find_folder_and_filename(bucket, csvPathname, returnFullPath=True)

        glm_doit(self, csvFilename, bucket, csvPathname, timeoutSecs=30)

        filename1x = "hastie_1x.data"
        pathname1x = SYNDATASETS_DIR + '/' + filename1x
        h2o_util.file_gunzip(fullPathname, pathname1x)
        
        filename1xShuf = "hastie_1x.data_shuf"
        pathname1xShuf = SYNDATASETS_DIR + '/' + filename1xShuf
        h2o_util.file_shuffle(pathname1x, pathname1xShuf)

        filename2x = "hastie_2x.data"
        pathname2x = SYNDATASETS_DIR + '/' + filename2x
        h2o_util.file_cat(pathname1xShuf, pathname1xShuf, pathname2x)

        filename2xShuf = "hastie_2x.data_shuf"
        pathname2xShuf = SYNDATASETS_DIR + '/' + filename2xShuf
        h2o_util.file_shuffle(pathname2x, pathname2xShuf)
        glm_doit(self, filename2xShuf, None, pathname2xShuf, timeoutSecs=45)

        # too big to shuffle?
        filename4x = "hastie_4x.data"
        pathname4x = SYNDATASETS_DIR + '/' + filename4x
        h2o_util.file_cat(pathname2xShuf,pathname2xShuf,pathname4x)
        glm_doit(self,filename4x, None, pathname4x, timeoutSecs=120)

if __name__ == '__main__':
    h2o.unit_main()
