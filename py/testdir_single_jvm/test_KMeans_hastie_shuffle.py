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

import os, json, unittest, time, shutil, sys
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_kmeans, h2o_util, h2o_hosts
import copy

def kmeans_doit(self, csvFilename, csvPathname, timeoutSecs=30):
    print "\nStarting KMeans of", csvFilename
    parseKey = h2o_cmd.parseFile(csvPathname=csvPathname, key2=csvFilename + ".hex", timeoutSecs=10)
    # hastie has two values, 1 and -1.
    # we could not specify cols, but this is more fun
    cols = ",".join(map(str,range(11)))
    kwargs = {
        'k': 1, 
        'epsilon': 1e-6,
        'cols': cols, 
        'destination_key': 'KMeansModel.hex'
    }
    start = time.time()
    kmeans = h2o_cmd.runKMeansOnly(parseKey=parseKey, \
        timeoutSecs=timeoutSecs, retryDelaySecs=2, pollTimeoutSecs=60, **kwargs)
    elapsed = time.time() - start
    print "kmeans end on ", csvPathname, 'took', elapsed, 'seconds.', \
        "%d pct. of timeout" % ((elapsed/timeoutSecs) * 100)
    h2o_kmeans.simpleCheckKMeans(self, kmeans, **kwargs)
    inspect = h2o_cmd.runInspect(None, key=kmeans['destination_key'])
    ### print h2o.dump_json(inspect)


    # compare this kmeans to the first one. since the files are replications, the results
    # should be similar?
    KMeansModel = inspect['KMeansModel']
    clusters = KMeansModel['clusters'][0]
    print "clusters:", h2o.dump_json(clusters)
    
    if self.clusters1:
        h2o_kmeans.compareToFirstKMeans(self, clusters, self.clusters1)
    else:
        self.clusters1 = copy.deepcopy(clusters)

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(1)
        else:
            h2o_hosts.build_cloud_with_hosts(1)
        global SYNDATASETS_DIR
        SYNDATASETS_DIR = h2o.make_syn_dir()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    clusters1 = {}
    def test_1mx10_hastie_10_2_cat_and_shuffle(self):
        # gunzip it and cat it to create 2x and 4x replications in SYNDATASETS_DIR
        # FIX! eventually we'll compare the 1x, 2x and 4x results like we do
        # in other tests. (catdata?)

        # This test also adds file shuffling, to see that row order doesn't matter
        csvFilename = "1mx10_hastie_10_2.data.gz"
        csvPathname = h2o.find_dataset('logreg' + '/' + csvFilename)
        kmeans_doit(self, csvFilename, csvPathname, timeoutSecs=60)

        filename1x = "hastie_1x.data"
        pathname1x = SYNDATASETS_DIR + '/' + filename1x
        h2o_util.file_gunzip(csvPathname, pathname1x)
        
        filename1xShuf = "hastie_1x.data_shuf"
        pathname1xShuf = SYNDATASETS_DIR + '/' + filename1xShuf
        h2o_util.file_shuffle(pathname1x, pathname1xShuf)

        filename2x = "hastie_2x.data"
        pathname2x = SYNDATASETS_DIR + '/' + filename2x
        h2o_util.file_cat(pathname1xShuf, pathname1xShuf, pathname2x)

        filename2xShuf = "hastie_2x.data_shuf"
        pathname2xShuf = SYNDATASETS_DIR + '/' + filename2xShuf
        h2o_util.file_shuffle(pathname2x, pathname2xShuf)
        kmeans_doit(self, filename2xShuf, pathname2xShuf, timeoutSecs=90)

        # too big to shuffle?
        filename4x = "hastie_4x.data"
        pathname4x = SYNDATASETS_DIR + '/' + filename4x
        h2o_util.file_cat(pathname2xShuf, pathname2xShuf, pathname4x)
        kmeans_doit(self, filename4x, pathname4x, timeoutSecs=120)

if __name__ == '__main__':
    h2o.unit_main()
