import unittest, random, sys, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_glm, h2o_import as h2i, h2o_util, h2o_exec as h2e

def define_params():
    paramDict = {
        'ignored_cols': [None, 0, 1],
        # response col must be categorical?
        # laplace smoothing parameter
        # between 0 and 100000
        'laplace': [None, 0, 1, 10, 1e5],
        'drop_na_cols': [None, 0, 1],
        'min_std_dev': [None, 1e-5],
        }
    return paramDict

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_bayes_and2(self):
        csvPathname = 'standard/covtype.data'
        parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, schema='put')
        paramDict = define_params()
        for trial in range(1):
            response = 'C55'
            params = {
                'response': response, 
                }

            colX = h2o_util.pickRandParams(paramDict, params)
            kwargs = params.copy()

            timeoutSecs = 120
            # chagne response to factor
            execExpr = 'covtype.hex[,54+1] = factor(covtype.hex[,54+1] != 5)' # turn 7-class problem into binomial such that AUC can work below..
            resultExec, ncols = h2e.exec_expr(execExpr=execExpr)

            start = time.time()
            bayesResult = h2o.nodes[0].naive_bayes(timeoutSecs=timeoutSecs, source='covtype.hex', **kwargs)
            print "bayes end on ", csvPathname, 'took', time.time() - start, 'seconds'

            print "bayes result:", h2o.dump_json(bayesResult)

            nb_model = bayesResult['nb_model']
            ncats = nb_model['ncats']
            nnums = nb_model['nnums']
            pcond = nb_model['pcond']
            pprior = nb_model['pprior']
            rescnt = nb_model['rescnt']
            modelClassDist = nb_model['_modelClassDist']
            names = nb_model['_names']
            domains = nb_model['_domains']
            priorClassDist = nb_model['_priorClassDist']
            model_key = nb_model['_key']


            # is it an error to get std dev of 0 after predicting?
            print "Doing predict with same dataset, and the bayes model"
            h2o.nodes[0].generate_predictions(model_key=model_key, data_key='covtype.hex', prediction='Predict.hex')

            # just get a predict and AUC on the same data. has to be binomial result
            resultAUC = h2o.nodes[0].generate_auc(thresholds=None, actual='covtype.hex', predict='Predict.hex',
                vactual=response, vpredict=1)
            print "AUC result:", h2o.dump_json(resultAUC)


            print "Trial #", trial, "completed\n"


if __name__ == '__main__':
    h2o.unit_main()
