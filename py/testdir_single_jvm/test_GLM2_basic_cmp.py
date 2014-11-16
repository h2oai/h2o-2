import unittest, random, sys, time
sys.path.extend(['.','..','../..','py'])

import h2o, h2o_cmd, h2o_import as h2i, h2o_exec, h2o_glm, h2o_jobs
import h2o_print as h2p
SCIPY_INSTALLED = True
try:
    import scipy as sp
    import numpy as np
    import sklearn as sk
    print "numpy, scipy and sklearn are installed. Will do extra checks"

except ImportError:
    print "numpy, scipy or sklearn is not installed. Will just do h2o stuff"
    SCIPY_INSTALLED = False

#*********************************************************************************
def do_scipy_glm(self, bucket, csvPathname, L, family='binomial'):
    
    h2p.red_print("Now doing sklearn")
    h2p.red_print("\nsee http://scikit-learn.org/0.11/modules/generated/sklearn.linear_model.LogisticRegression.html#sklearn.linear_model.LogisticRegression")

    import numpy as np
    import scipy as sp
    from sklearn.linear_model import LogisticRegression
    from numpy import loadtxt

    csvPathnameFull = h2i.find_folder_and_filename(bucket, csvPathname, returnFullPath=True)

    # make sure it does fp divide
    C = 1/(L+0.0)
    print "C regularization:", C
    dataset = np.loadtxt( 
        open(csvPathnameFull,'r'),
        skiprows=1, # skip the header
        delimiter=',',
        dtype='float');

    print "\ncsv read for training, done"

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

    if family!='binomial':
        raise Exception("Only have binomial logistic for scipy")
    print "\nTrying l2"
    clf2 = LogisticRegression(
        C=C,
        dual=False, 
        fit_intercept=True, 
        intercept_scaling=1, 
        penalty='l2', 
        tol=0.0001);

    # train the classifier
    start = time.time()
    clf2.fit(train, target)
    print "L2 fit took", time.time() - start, "seconds"

    # print "coefficients:", clf2.coef_
    cstring = "".join([("%.5e  " % c) for c in clf2.coef_[0]])
    h2p.green_print("sklearn L2 C", C)
    h2p.green_print("sklearn coefficients:", cstring)
    h2p.green_print("sklearn intercept:", "%.5e" % clf2.intercept_[0])
    h2p.green_print("sklearn score:", clf2.score(train,target))

    print "\nTrying l1"
    clf1 = LogisticRegression(
        C=C,
        dual=False, 
        fit_intercept=True, 
        intercept_scaling=1, 
        penalty='l1', 
        tol=0.0001);

    # train the classifier
    start = time.time()
    clf1.fit(train, target)
    print "L1 fit took", time.time() - start, "seconds"

    # print "coefficients:", clf1.coef_
    cstring = "".join([("%.5e  " % c) for c in clf1.coef_[0]])
    h2p.green_print("sklearn L1 C", C)
    h2p.green_print("sklearn coefficients:", cstring)
    h2p.green_print("sklearn intercept:", "%.5e" % clf1.intercept_[0])
    h2p.green_print("sklearn score:", clf1.score(train,target))

    # attributes are accessed in the normal python way
    dx = clf1.__dict__
    dx.keys()

    ##    ['loss', 'C', 'dual', 'fit_intercept', 'class_weight_label', 'label_', 
    ##     'penalty', 'multi_class', 'raw_coef_', 'tol', 'class_weight', 
    ##     'intercept_scaling']

#*********************************************************************************
def do_h2o_glm(self, bucket, csvPathname, L, family='binomial'):

    h2p.red_print("\nNow doing h2o")
    parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, schema='local', timeoutSecs=180)
    # save the resolved pathname for use in the sklearn csv read below

    inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
    print inspect
    print "\n" + csvPathname, \
        "    numRows:", "{:,}".format(inspect['numRows']), \
        "    numCols:", "{:,}".format(inspect['numCols'])

    x         = 'ID'
    y         = 'CAPSULE'
    family    = family
    alpha     = '0'
    lambda_   = L
    nfolds    = '0'
    f         = 'prostate'
    modelKey  = 'GLM_' + f

    kwargs = {
        'response'           : y,
        'ignored_cols'       : x,
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

    def test_GLM2_basic_cmp(self):
        bucket = 'smalldata'
        importFolderPath = "logreg"
        csvFilename = 'prostate.csv'
        csvPathname = importFolderPath + "/" + csvFilename

        # use L for lambda in h2o, C=1/L in sklearn
        family = 'binomial'
        L = 1e-4
        do_h2o_glm(self, bucket, csvPathname, L, family)
        if SCIPY_INSTALLED:
            do_scipy_glm(self, bucket, csvPathname, L, family)

        # since we invert for C, can't use 0 (infinity)
        L = 1e-13
        # C in sklearn Specifies the strength of the regularization. 
        # The smaller it is the bigger in the regularization. 
        # we'll set it to 1/L
        do_h2o_glm(self, bucket, csvPathname, L, family)
        if SCIPY_INSTALLED:
            do_scipy_glm(self, bucket, csvPathname, L, family)


if __name__ == '__main__':
    h2o.unit_main()
