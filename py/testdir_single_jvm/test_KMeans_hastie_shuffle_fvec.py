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
import h2o, h2o_cmd, h2o_kmeans, h2o_util, h2o_import as h2i

def kmeans_doit(self, csvFilename, bucket, csvPathname, numRows, timeoutSecs=30):
    print "\nStarting KMeans of", csvFilename
    parseResult = h2i.import_parse(bucket=bucket, path=csvPathname, schema='put', hex_key=csvFilename + ".hex", timeoutSecs=20)
    # hastie has two values, 1 and -1.
    # we could not specify cols, but this is more fun
    kwargs = {
        'k': 1, 
        'initialization': 'Furthest',
        'destination_key': 'KMeansModel.hex',
        'max_iter': 25,
        # reuse the same seed, to get deterministic results (otherwise sometimes fails
        'seed': 265211114317615310,
    }
    start = time.time()
    kmeans = h2o_cmd.runKMeans(parseResult=parseResult, \
        timeoutSecs=timeoutSecs, retryDelaySecs=2, pollTimeoutSecs=60, **kwargs)
    elapsed = time.time() - start
    print "kmeans end on ", csvPathname, 'took', elapsed, 'seconds.', \
        "%d pct. of timeout" % ((elapsed/timeoutSecs) * 100)

    (centers, tupleResultList) = h2o_kmeans.bigCheckResults(self, kmeans, csvPathname, parseResult, 'd', **kwargs)

    expected = [
        ([-0.0006628900000000158, -0.0004671200060434639, 0.0009330300069879741, 0.0007883800000000272, 0.0007548200000000111, 0.0005617899864856153, 0.0013246499999999897, 0.0004036299999999859, -0.0014307100000000314, 0.0021324000161308796, 0.00154], numRows, None)
    ]
    # all are multipliers of expected tuple value
    allowedDelta = (0.01, 0.01, 0.01)
    h2o_kmeans.compareResultsToExpected(self, tupleResultList, expected, allowedDelta, trial=0)

    # compare this kmeans to the first one. since the files are replications, the results
    # should be similar?
    # inspect doesn't work
    # inspect = h2o_cmd.runInspect(None, key=kmeans['model']['_key'])
    # KMeansModel = inspect['KMeansModel']
    modelView = h2o.nodes[0].kmeans_view(model='KMeansModel.hex')
    h2o.verboseprint("KMeans2ModelView:", h2o.dump_json(modelView))
    model = modelView['model']
    clusters = model['centers']
    within_cluster_variances = model['within_cluster_variances']
    total_within_SS = model['total_within_SS']
    print "within_cluster_variances:", within_cluster_variances
    print "total_within_SS:", total_within_SS
    
    if self.clusters1:
        h2o_kmeans.compareToFirstKMeans(self, clusters, self.clusters1)
    else:
        self.clusters1 = copy.deepcopy(clusters)

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

    clusters1 = []
    def test_KMeans_hastie_shuffle_fvec(self):
        # gunzip it and cat it to create 2x and 4x replications in SYNDATASETS_DIR
        # FIX! eventually we'll compare the 1x, 2x and 4x results like we do
        # in other tests. (catdata?)

        # This test also adds file shuffling, to see that row order doesn't matter
        csvFilename = "1mx10_hastie_10_2.data.gz"
        csvPathname = 'standard/' + csvFilename
        bucket = 'home-0xdiag-datasets'
        kmeans_doit(self, csvFilename, bucket, csvPathname, numRows=1000000, timeoutSecs=60)
        fullPathname = h2i.find_folder_and_filename(bucket, csvPathname, returnFullPath=True)

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
        kmeans_doit(self, filename2xShuf, None, pathname2xShuf, numRows=2000000, timeoutSecs=90)

        # too big to shuffle?
        filename4x = "hastie_4x.data"
        pathname4x = SYNDATASETS_DIR + '/' + filename4x
        h2o_util.file_cat(pathname2xShuf, pathname2xShuf, pathname4x)
        kmeans_doit(self, filename4x, None, pathname4x, numRows=4000000, timeoutSecs=120)

if __name__ == '__main__':
    h2o.unit_main()
