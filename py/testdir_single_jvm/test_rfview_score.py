import unittest, random, sys, time
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_rf, h2o_hosts, h2o_import as h2i

# we can pass ntree thru kwargs if we don't use the "trees" parameter in runRF
# only classes 1-7 in the 55th col
# don't allow None on ntree..causes 50 tree default!
print "Temporarily not using bin_limit=1 to 4"
paramDict = {
    'response_variable': [None,54],
    # individual weights can make the error go very high and fail the check below
    'class_weights': [None,
        '1=20,2=20,3=20,4=20,5=20,6=20,7=20',
        '1=10,2=10,3=20,4=20,5=20,6=20,7=20'],
    'ntree': [50],
    'model_key': ['model_keyA', '012345', '__hello'],
    # UPDATE: H2O...OOBE has to be 0 for scoring
    'out_of_bag_error_estimate': [0],
    'stat_type': [None, 'ENTROPY', 'GINI'],
    'depth': [None, 1,10,20,100],
    'bin_limit': [None,5,10,100,1000],
    'ignore': [None,0,1,2,3,4,5,6,7,8,9],
    'sample': [None,20,40,60,80,90],
    'seed': [None,'0','1','11111','19823134','1231231'],
    # stack trace if we use more features than legal. dropped or redundanct cols reduce 
    # legal max also.
    # only 51 non-constant cols in the 20k covtype?
    'features': [None,1,3,5,7,9,11,13,17,19,23,37,51],
    'exclusive_split_limit': [None,0,3,5],
    'sampling_strategy': [None, 'RANDOM', 'STRATIFIED_LOCAL' ],
    'strata_samples': [
        None,
        "2=10",
        "1=5",
        "2=3",
        "1=1,2=1,3=1,4=1,5=1,6=1,7=1",
        "1=100,2=100,3=100,4=100,5=100,6=100,7=100",
        "1=0,2=0,3=0,4=0,5=0,6=0,7=0",
        ]
    }

print "Will RF train on one dataset, test on another (multiple params)"
class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED, localhost
        SEED = h2o.setup_random_seed()
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(node_count=1)
        else:
            h2o_hosts.build_cloud_with_hosts(node_count=1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_rfview_score(self):
        csvPathnameTrain = 'standard/covtype.data'
        print "Train with:", csvPathnameTrain
        parseResultTrain = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathnameTrain, schema='put', 
            hex_key="covtype.hex", timeoutSecs=15)
        dataKeyTrain = parseResultTrain['destination_key']

        csvPathnameTest = 'standard/covtype.data'
        print "Test with:", csvPathnameTest
        parseResultTest = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathnameTest, schema='put', 
            hex_key="covtype.hex", timeoutSecs=15)
        dataKeyTest = parseResultTest['destination_key']

        for trial in range(5):
            # params is mutable. This is default.
            params = {'ntree': 13, 'out_of_bag_error_estimate': 0}
            colX = h2o_rf.pickRandRfParams(paramDict, params)
            kwargs = params.copy()
            # adjust timeoutSecs with the number of trees
            # seems ec2 can be really slow
            timeoutSecs = 30 + kwargs['ntree'] * 10
            rfv = h2o_cmd.runRF(parseResult=parseResultTrain, timeoutSecs=timeoutSecs, retryDelaySecs=1, **kwargs)
            ### print "rf response:", h2o.dump_json(rfv)

            model_key = rfv['model_key']
            # pop the stuff from kwargs that were passing as params
            kwargs.pop('model_key',None)

            data_key = rfv['data_key']
            kwargs.pop('data_key',None)

            ntree = rfv['ntree']
            kwargs.pop('ntree',None)
            # scoring
            # RFView.html?
            # dataKeyTest=a5m.hex&
            # model_key=__RFModel_81c5063c-e724-4ebe-bfc1-3ac6838bc628&
            # response_variable=1&
            # ntree=50&
            # class_weights=-1%3D1.0%2C0%3D1.0%2C1%3D1.0&
            # out_of_bag_error_estimate=1&
            rfView = h2o_cmd.runRFView(None, dataKeyTest, model_key, ntree, 
                timeoutSecs, retryDelaySecs=1, **kwargs)
            # new web page for predict? throw it in here for now

            (classification_error, classErrorPctList, totalScores) = h2o_rf.simpleCheckRFView(rfv=rfView, ntree=ntree)
            # don't check error if stratified
            if 'sampling_strategy' in kwargs and kwargs['sampling_strategy'] != 'STRATIFIED_LOCAL':
                check_err = True
            else:
                check_err = False

            if check_err:
                self.assertAlmostEqual(classification_error, 0.03, delta=0.5, msg="Classification error %s differs too much" % classification_error)

            start = time.time()
            predict = h2o.nodes[0].generate_predictions(model_key=model_key, data_key=dataKeyTest)
            elapsed = time.time() - start
            print "predict end on ", dataKeyTest, 'took', elapsed, 'seconds.'

            kwargs['iterative_cm'] = 0
            rfView = h2o_cmd.runRFView(None, dataKeyTest, model_key, ntree,
                timeoutSecs, retryDelaySecs=1, print_params=True, **kwargs)
            # FIX! should update this expected classification error
            (classification_error, classErrorPctList, totalScores) = h2o_rf.simpleCheckRFView(rfv=rfView, ntree=ntree)
            # don't check error if stratified
            if check_err:
                self.assertAlmostEqual(classification_error, 0.03, delta=0.5, msg="Classification error %s differs too much" % classification_error)
            start = time.time()
            predict = h2o.nodes[0].generate_predictions(model_key=model_key, data_key=dataKeyTest)
            elapsed = time.time() - start
            print "predict end on ", dataKeyTest, 'took', elapsed, 'seconds.'

            kwargs['iterative_cm'] = 1
            rfView = h2o_cmd.runRFView(None, dataKeyTest, model_key, ntree, 
                timeoutSecs, retryDelaySecs=1, print_params=True, **kwargs)
            # FIX! should update this expected classification error
            (classification_error, classErrorPctList, totalScores) = h2o_rf.simpleCheckRFView(rfv=rfView, ntree=ntree)
            # don't check error if stratified
            if check_err:
                self.assertAlmostEqual(classification_error, 0.03, delta=0.5, msg="Classification error %s differs too much" % classification_error)
            start = time.time()
            predict = h2o.nodes[0].generate_predictions(model_key=model_key, data_key=dataKeyTest)
            elapsed = time.time() - start
            print "predict end on ", dataKeyTest, 'took', elapsed, 'seconds.'

            kwargs['iterative_cm'] = 1
            kwargs['class_weights'] = '1=1,2=2,3=3,4=4,5=5,6=6,7=7'
            rfView = h2o_cmd.runRFView(None, dataKeyTest, model_key, ntree,
                timeoutSecs, retryDelaySecs=1, print_params=True, **kwargs)
            # FIX! should update this expected classification error
            (classification_error, classErrorPctList, totalScores) = h2o_rf.simpleCheckRFView(rfv=rfView, ntree=ntree)
            # don't check error if stratified
            if check_err:
                self.assertAlmostEqual(classification_error, 0.03, delta=0.5, msg="Classification error %s differs too much" % classification_error)
            start = time.time()
            predict = h2o.nodes[0].generate_predictions(model_key=model_key, data_key=dataKeyTest)
            elapsed = time.time() - start
            print "predict end on ", dataKeyTest, 'took', elapsed, 'seconds.'

            print "Trial #", trial, "completed"

if __name__ == '__main__':
    h2o.unit_main()
