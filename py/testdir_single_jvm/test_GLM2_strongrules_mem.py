import unittest, sys, time
sys.path.extend(['.','..','py'])
import h2o_hosts, h2o_glm
import h2o, h2o_cmd, h2o_import as h2i

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(java_heap_GB=1)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()


# possible params
#             'strong_rules_enabled'
#             'lambda_search'
#             'nlambdas'
#             'lambda_min_ratio'
#             'prior'
#             'source'
#             'destination_key'
#             'response'
#             'cols'
#             'ignored_cols'
#             'ignored_cols_by_name'
#             'max_iter'
#             'standardize'
#             'family'
#             'alpha'
#             'lambda'
#             'beta_epsilon'
#             'tweedie_variance_power'
#             'n_folds'
# 
#             # only GLMGrid has this..we should complain about it on GLM?
#             'parallelism'
#             'beta_eps'
#             'higher_accuracy'
#             'use_all_factor_levels'

    def test_GLM2_strongrules_mem(self):
        csvFilename = 'AirlinesTrain.csv.zip'
        csvPathname = 'airlines'+'/' + csvFilename
        parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, schema='put', timeoutSecs=15)
        params = {
            'strong_rules_enabled': 1,
            'response': 'IsDepDelayed', 
            'ignored_cols': 'IsDepDelayed_REC', 
            'family': 'binomial',
        }
        kwargs = params.copy()

        starttime = time.time()
        glmtest = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=15, **kwargs)
        elapsedtime = time.time() - starttime 
        print("glm took",elapsedtime)
        h2o_glm.simpleCheckGLM(self, glmtest, None, **kwargs)


if __name__ == '__main__':
    h2o.unit_main()
