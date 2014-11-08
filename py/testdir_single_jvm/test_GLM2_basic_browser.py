import unittest, random, sys, time
sys.path.extend(['.','..','../..','py'])

import h2o, h2o_cmd, h2o_import as h2i, h2o_exec, h2o_glm, h2o_jobs, h2o_browse as h2b

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init(1, java_heap_GB=10)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GLM2_basic_browser(self):
        h2b.browseTheCloud()

        importFolderPath = "logreg"
        csvFilename = 'prostate.csv'
        csvPathname = importFolderPath + "/" + csvFilename
        hex_key = csvFilename + ".hex"

        parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, schema='local', hex_key=hex_key, timeoutSecs=180)
        inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
        print inspect
        print "\n" + csvPathname, \
            "    numRows:", "{:,}".format(inspect['numRows']), \
            "    numCols:", "{:,}".format(inspect['numCols'])

        x         = 'ID'
        y         = 'CAPSULE'
        family    = 'binomial'
        alpha     = '0.5'
        lambda_   = '1E-4'
        nfolds    = '0'
        f         = 'prostate'
        modelKey  = 'GLM_' + f

        kwargs = {       'response'           : y,
                         'ignored_cols'       : x,
                         'family'             : family,
                         'lambda'             : lambda_,
                         'alpha'              : alpha,
                         'n_folds'            : nfolds, # passes if 0, fails otherwise
                         'destination_key'    : modelKey,
                 }

        timeoutSecs = 60
        start = time.time()
        glmResult = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=timeoutSecs, retryDelaySecs=0.25, pollTimeoutSecs=180, **kwargs)

        # this stuff was left over from when we got the result after polling the jobs list
        # okay to do it again
        # GLM2: when it redirects to the model view, we no longer have the job_key! (unlike the first response and polling)
        if 1==0:
            job_key = glmResult['job_key']
            # is the job finishing before polling would say it's done?
            params = {'job_key': job_key, 'destination_key': modelKey}
            glm = h2o.nodes[0].completion_redirect(jsonRequest="2/GLMProgressPage2.json", params=params)
            print "GLM result from completion_redirect:", h2o.dump_json(a)
        if 1==1:
            glm = h2o.nodes[0].glm_view(_modelKey=modelKey)
            ### print "GLM result from glm_view:", h2o.dump_json(a)

        h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)

        glm_model = glm['glm_model']
        _names = glm_model['_names']
        coefficients_names = glm_model['coefficients_names']
        submodels = glm_model['submodels'][0]

        beta = submodels['beta']
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

        # now redo it all thru the browser
        h2b.browseJsonHistoryAsUrl()


if __name__ == '__main__':
    h2o.unit_main()
