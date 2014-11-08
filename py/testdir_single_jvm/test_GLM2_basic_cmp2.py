import unittest, random, sys, time
sys.path.extend(['.','..','../..','py'])

import h2o, h2o_cmd, h2o_import as h2i, h2o_exec, h2o_glm, h2o_jobs
import h2o_print as h2p
SCIPY_INSTALLED = True
try:
    import scipy as sp
    import numpy as np
    import sklearn as sk
    import statsmodels as sm
    import statsmodels.api as sm_api
    print "numpy, scipy and sklearn are installed. Will do extra checks"

except ImportError:
    print "numpy, sklearn, or statsmodels  is not installed. Will just do h2o stuff"
    SCIPY_INSTALLED = False

# http://statsmodels.sourceforge.net/devel/glm.html#module-reference
# This seems better than the sklearn LogisticRegression I was using
# Using Logit is as simple as thishttp://statsmodels.sourceforge.net/devel/examples/generated/example_discrete.html
#*********************************************************************************
def do_statsmodels_glm(self, bucket, csvPathname, L, family='gaussian'):
    
    h2p.red_print("Now doing statsmodels")
    h2p.red_print("http://statsmodels.sourceforge.net/devel/glm.html#module-reference")
    h2p.red_print("http://statsmodels.sourceforge.net/devel/generated/statsmodels.genmod.generalized_linear_model.GLM.html")

    import numpy as np
    import scipy as sp
    from numpy import loadtxt
    import statsmodels as sm

    csvPathnameFull = h2i.find_folder_and_filename(bucket, csvPathname, returnFullPath=True)

    if 1==1:
        dataset = np.loadtxt( 
            open(csvPathnameFull,'r'),
            skiprows=1, # skip the header
            delimiter=',',
            dtype='float');

    # skipping cols from the begining... (ID is col 1)
    # In newer versions of Numpy, np.genfromtxt can take an iterable argument, 
    # so you can wrap the file you're reading in a generator that generates lines, 
    # skipping the first N columns. If your numbers are comma-separated, that's something like
    if 1==0:
        f = open(csvPathnameFull,'r'),
        np.genfromtxt(
            (",".join(ln.split()[1:]) for ln in f),
            skiprows=1, # skip the header
            delimiter=',',
            dtype='float');

    print "\ncsv read for training, done"

    # data is last column
    # drop the output
    n_features = len(dataset[0]) - 1;
    print "n_features:", n_features

    # don't want ID (col 0) or CAPSULE (col 1)
    # get CAPSULE
    target = [x[1] for x in dataset]
    # slice off the first 2
    train  = np.array ( [x[2:] for x in dataset] )

    n_samples, n_features = train.shape
    print "n_samples:", n_samples, "n_features:",  n_features

    print "histogram of target"
    print sp.histogram(target,3)

    print "len(train):",  len(train)
    print "len(target):", len(target)
    print "dataset shape:", dataset.shape

    if family!='gaussian':
        raise Exception("Only have gaussian logistic for scipy")

    # train the classifier
    gauss_log = sm_api.GLM(target, train, family=sm_api.families.Gaussian(sm_api.families.links.log))
    start = time.time()
    gauss_log_results = gauss_log.fit()
    print "sm_api.GLM took", time.time() - start, "seconds"
    print gauss_log_results.summary()


#*********************************************************************************
def do_h2o_glm(self, bucket, csvPathname, L, family='gaussian'):

    h2p.red_print("\nNow doing h2o")
    parseResult = h2i.import_parse(bucket=bucket, path=csvPathname, schema='local', timeoutSecs=180)
    # save the resolved pathname for use in the sklearn csv read below

    inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
    print inspect
    print "\n" + csvPathname, \
        "    numRows:", "{:,}".format(inspect['numRows']), \
        "    numCols:", "{:,}".format(inspect['numCols'])

    # Need to chop out the ID col?
    # x         = 'ID'
    # y         = 'CAPSULE'
    family    = family
    alpha     = '0'
    lambda_   = L
    nfolds    = '0'
    modelKey  = 'GLM_Model'
    y         = 'GLEASON'
    

    kwargs = {
        'response'           : y,
        'ignored_cols'       : 'ID, CAPSULE',
        'family'             : family,
        'lambda'             : lambda_,
        'alpha'              : alpha,
        'n_folds'            : nfolds, # passes if 0, fails otherwise
        'destination_key'    : modelKey,
    }

    timeoutSecs = 60
    start = time.time()
    glmResult = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=timeoutSecs, **kwargs)

    # this stuff was left over from when we got the result after polling the jobs list
    # okay to do it again
    # GLM2: when it redirects to the model view, we no longer have the job_key! (unlike the first response and polling)
    (warnings, clist, intercept) = h2o_glm.simpleCheckGLM(self, glmResult, None, **kwargs)
    cstring = "".join([("%.5e  " % c) for c in clist])
    h2p.green_print("h2o alpha ", alpha)
    h2p.green_print("h2o lambda ", lambda_)
    h2p.green_print("h2o coefficient list:", cstring)
    h2p.green_print("h2o intercept", "%.5e  " %  intercept)

    # other stuff in the json response
    glm_model = glmResult['glm_model']
    _names = glm_model['_names']
    coefficients_names = glm_model['coefficients_names']

    # the first submodel is the right one, if onely one lambda is provided as a parameter above
    submodels = glm_model['submodels'][0]

    beta = submodels['beta']
    h2p.red_print("beta:", beta)
    norm_beta = submodels['norm_beta']
    iteration = submodels['iteration']

    validation = submodels['validation']
    auc = validation['auc']
    aic = validation['aic']
    null_deviance = validation['null_deviance']
    residual_deviance = validation['residual_deviance']

    print '_names', _names
    print 'coefficients_names', coefficients_names
    # did beta get shortened? the simple check confirms names/beta/norm_beta are same length
    print 'beta', beta
    print 'iteration', iteration
    print 'auc', auc

#*********************************************************************************
# the actual test that will run both
#*********************************************************************************
class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init(1, java_heap_GB=10)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GLM2_basic_cmp2(self):

        if 1==1:
            bucket = 'smalldata'
            importFolderPath = "logreg"
            csvFilename = 'prostate.csv'
        if 1==0:
            bucket = 'home-0xdiag-datasets'
            importFolderPath = "standard"
            csvFilename = 'covtype.data'

        csvPathname = importFolderPath + "/" + csvFilename

        # use L for lambda in h2o, C=1/L in sklearn
        family = 'gaussian'
        L = 1e-4
        do_h2o_glm(self, bucket, csvPathname, L, family)
        if SCIPY_INSTALLED:
            do_statsmodels_glm(self, bucket, csvPathname, L, family)

        # since we invert for C, can't use 0 (infinity)
        L = 0
        do_h2o_glm(self, bucket, csvPathname, L, family)
        if SCIPY_INSTALLED:
            do_statsmodels_glm(self, bucket, csvPathname, L, family)


if __name__ == '__main__':
    h2o.unit_main()
