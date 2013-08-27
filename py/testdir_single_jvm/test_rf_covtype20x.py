import unittest, random, sys, time
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_rf, h2o_hosts, h2o_import as h2i, h2o_jobs

# we can pass ntree thru kwargs if we don't use the "trees" parameter in runRF
# only classes 1-7 in the 55th col
# don't allow None on ntree..causes 50 tree default!
print "Temporarily not using bin_limit=1 to 4"
print "Use out_of_bag_error_estimate=0, to get full scoring, for when we change datasets"
paramDict = {
    'response_variable': [None,54],
    'class_weights': [None,'1=2','2=2','3=2','4=2','5=2','6=2','7=2'], 'ntree': [5], 
    # UPDATE: H2O...OOBE has to be 0 for scoring
    'out_of_bag_error_estimate': [0],
    'stat_type': [None, 'ENTROPY', 'GINI'],
    'depth': [None, 1,10,20,100],
    'bin_limit': [None,5,10,100,1000],
    'parallel': [None,0,1],
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
        "1=99,2=99,3=99,4=99,5=99,6=99,7=99",
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
            h2o.build_cloud(node_count=1, java_heap_GB=14)
        else:
            h2o_hosts.build_cloud_with_hosts(node_count=1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_rf_covtype20x(self):
        importFolderPath = '/home/0xdiag/datasets/standard'

        importFolderResult = h2i.setupImportFolder(None, importFolderPath)
        csvFilenameTrain = 'covtype20x.data'
        key2 = 'covtype20x.data.A.hex'
        parseResultTrain = h2i.parseImportFolderFile(None, csvFilenameTrain, importFolderPath, key2=key2, timeoutSecs=500)
        print csvFilenameTrain, 'parse time:', parseResultTrain['response']['time']
        inspect = h2o_cmd.runInspect(key=parseResultTrain['destination_key'])
        dataKeyTrain = parseResultTrain['destination_key']
        print "Parse end", dataKeyTrain

        # have to re import since source key is gone
        # we could just copy the key, but sometimes we change the test/train data  to covtype.data
        importFolderResult = h2i.setupImportFolder(None, importFolderPath)
        csvFilenameTest = 'covtype20x.data'
        key2 = 'covtype20x.data.B.hex'
        parseResultTest = h2i.parseImportFolderFile(None, csvFilenameTest, importFolderPath, key2=key2, timeoutSecs=500)
        print csvFilenameTest, 'parse time:', parseResultTest['response']['time']
        print "Parse result['destination_key']:", parseResultTest['destination_key']
        inspect = h2o_cmd.runInspect(key=parseResultTest['destination_key'])
        dataKeyTest = parseResultTest['destination_key']
        dataKeyTest2 = 'covtype20x.data.C.hex'

        print "Parse end", dataKeyTest
        
        # make a 3rd key so the predict is uncached too!
        execExpr = dataKeyTest2 + "=" + dataKeyTest
        resultExec = h2o_cmd.runExecOnly(expression=execExpr, timeoutSecs=15)

        # train
        # this does RFView to understand when RF completes, so the time reported for RFView here, should be 
        # considered the "first RFView" times..subsequent have some caching?. 
        # unless the no_confusion_matrix works

        # params is mutable. This is default.
        print "RF with no_confusion_matrix=1, so we can 'time' the RFView separately after job completion?"
        params = {
            'ntree': 6, 
            'parallel': 1, 
            'out_of_bag_error_estimate': 0, 
            'no_confusion_matrix': 1,
            'model_key': 'RF_model'
        }

        colX = h2o_rf.pickRandRfParams(paramDict, params)
        kwargs = params.copy()
        # adjust timeoutSecs with the number of trees
        # seems ec2 can be really slow
        timeoutSecs = 30 + kwargs['ntree'] * 60 * (kwargs['parallel'] and 1 or 5)

        start = time.time()
        rfv = h2o_cmd.runRFOnly(parseResult=parseResultTrain,
            timeoutSecs=timeoutSecs, retryDelaySecs=1, noPoll=True, **kwargs)
        print "rf job dispatch end on ", dataKeyTrain, 'took', time.time() - start, 'seconds'
        ### print "rf response:", h2o.dump_json(rfv)


        start = time.time()
        h2o_jobs.pollWaitJobs(pattern='RF_model', timeoutSecs=180, pollTimeoutSecs=500, retryDelaySecs=5)
        print "rf job end on ", dataKeyTrain, 'took', time.time() - start, 'seconds'

        print "\nRFView start after job completion"
        model_key = kwargs['model_key']
        ntree = kwargs['ntree']
        start = time.time()
        h2o_cmd.runRFView(None, dataKeyTrain, model_key, ntree, timeoutSecs)
        print "First rfview end on ", dataKeyTrain, 'took', time.time() - start, 'seconds'

        for trial in range(3):
            # scoring
            start = time.time()
            h2o_cmd.runRFView(None, dataKeyTest, model_key, ntree, timeoutSecs, out_of_bag_error_estimate=0, retryDelaySecs=1)
            print "rfview", trial, "end on ", dataKeyTest, 'took', time.time() - start, 'seconds.'

            start = time.time()
            predict = h2o.nodes[0].generate_predictions(model_key=model_key, data_key=dataKeyTest2)
            print "predict", trial, "end on ", dataKeyTest, 'took', time.time() - start, 'seconds.'

            print "Trial #", trial, "completed"

if __name__ == '__main__':
    h2o.unit_main()
