import unittest, time, sys
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_glm, h2o_import as h2i, h2o_jobs, h2o_gbm

DO_CLASSIFICATION = True
DO_FAIL_CASE = False
DO_FROM_TO_STEP = False

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init(1, java_heap_GB=1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GBMGrid_basic_many(self):
        csvFilename = "prostate.csv"
        print "\nStarting", csvFilename
        # columns start at 0
        csvPathname = 'logreg/' + csvFilename
        parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, hex_key=csvFilename + ".hex", schema='put')
        colNames = ['ID','CAPSULE','AGE','RACE','DPROS','DCAPS','PSA','VOL','GLEASON']

        modelKey = 'GBMGrid_prostate'
        # 'cols', 'ignored_cols_by_name', and 'ignored_cols' have to be exclusive
        params = {
            'destination_key': modelKey,
            'ignored_cols_by_name': 'ID',
            'learn_rate': '.1,.2',
            'ntrees': '8,10',
            'max_depth': '8,9',
            'min_rows': '1,2',
            'response': 'CAPSULE',
            'classification': 1 if DO_CLASSIFICATION else 0,
            'grid_parallelism': 1,
            }

        kwargs = params.copy()
        timeoutSecs = 1800
    
        jobs = []
        # kick off 5 of these GBM grid jobs, with different tree choices
        start = time.time()
        totalGBMGridJobs = 0
        # for more in range(8):
        # fast
        # for more in range(9):
        for i in range(5):
            kwargs = params.copy()
            kwargs['min_rows'] = '1,2,3'
            if DO_FROM_TO_STEP:
                kwargs['max_depth'] = '5:10:1'
            else:
                kwargs['max_depth'] = '5,6,10'

            GBMResult = h2o_cmd.runGBM(parseResult=parseResult, noPoll=True, **kwargs)
            # print "GBMResult:", h2o.dump_json(GBMResult)
            job_key = GBMResult['job_key']
            model_key = GBMResult['destination_key']
            jobs.append( (job_key, model_key) )
            totalGBMGridJobs += 1

        h2o_jobs.pollWaitJobs(timeoutSecs=300)
        elapsed = time.time() - start

        for job_key, model_key  in jobs:
            GBMResult = h2o.nodes[0].gbm_grid_view(job_key=job_key, destination_key=model_key)
            h2o_gbm.showGBMGridResults(GBMResult, 15)

        print "All GBM jobs completed in", elapsed, "seconds."
        print "totalGBMGridJobs:", totalGBMGridJobs

if __name__ == '__main__':
    h2o.unit_main()
