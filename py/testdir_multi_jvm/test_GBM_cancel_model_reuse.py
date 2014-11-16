import unittest, time, sys, random
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i, h2o_jobs, h2o_gbm


print "reuse the model key that you just cancelled"
DELETE_KEYS = True
# FIX need to get exec workin
DO_CLASSIFICATION = False

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init(3, java_heap_GB=4)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GBM_cancel_model_reuse(self):
        importFolderPath = 'standard'
        timeoutSecs = 500
        csvFilenameAll = [
            # have to use col name for response?
            ("manyfiles-nflx-gz", "file_1.dat.gz", 378),
            # ("manyfiles-nflx-gz", "file_[1-9].dat.gz", 378),
            # ("standard", "covtype.data", 54),
            # ("standard", "covtype20x.data", 54),
            ]
        # csvFilenameList = random.sample(csvFilenameAll,1)
        csvFilenameList = csvFilenameAll

        # pop open a browser on the cloud
        # h2b.browseTheCloud()

        for (importFolderPath, csvFilename, response) in csvFilenameList:
            # creates csvFilename.hex from file in importFolder dir 
            csvPathname = importFolderPath + "/" + csvFilename 
            print "FIX! is this guy getting cancelled because he's reusing a key name? but it should be okay?"
            (importResult, importPattern) = h2i.import_only(bucket='home-0xdiag-datasets', path=csvPathname, schema='local', 
                timeoutSecs=50)
            parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, schema='local', hex_key='c.hex', 
                timeoutSecs=500, noPoll=False, doSummary=False) # can't do summary until parse result is correct json

            h2o.check_sandbox_for_errors()

            # wait for it to show up in jobs?
            ## time.sleep(2)
            # no pattern waits for all
            ## h2o_jobs.pollWaitJobs(pattern=None, timeoutSecs=300, pollTimeoutSecs=10, retryDelaySecs=5)
            # print "\nparseResult", h2o.dump_json(parseResult)
            print "Parse result['destination_key']:", parseResult['destination_key']
            ## What's wrong here? too big?
            ### inspect = h2o_cmd.runInspect(key=parseResult['destination_key'], timeoutSecs=30, verbose=True)

            h2o.check_sandbox_for_errors()

            # have to avoid this on nflx data. colswap with exec
            # Exception: rjson error in gbm: Argument 'response' error: 
            # Only integer or enum/factor columns can be classified

            if DO_CLASSIFICATION:
                # need to flip the right col! (R wise)
                execExpr = 'c.hex[,%s]=c.hex[,%s]>15' % (response+1,response+1)
                kwargs = { 'str': execExpr }
                resultExec = h2o_cmd.runExec(**kwargs)

            # lets look at the response column now
            s = h2o_cmd.runSummary(key="c.hex", cols=response, max_ncols=1)
            # x = range(542)
            # remove the output too! (378)
            ignoreIndex = [3, 4, 5, 6, 7, 8, 9, 10, 11, 14, 16, 17, 18, 19, 20, 424, 425, 426, 540, 541, response]
            # have to add 1 for col start with 1, now. plus the C
            xIgnore = ",".join(["C" + str(i+1) for i in ignoreIndex])

            params = {
                'destination_key': None,
                'ignored_cols_by_name': xIgnore,
                'learn_rate': .1,
                'ntrees': 2,
                'max_depth': 8,
                'min_rows': 1,
                'response': "C" + str(response+1),
                'classification': 1 if DO_CLASSIFICATION else 0,
                'grid_parallelism': 4,
                }

            kwargs = params.copy()
            timeoutSecs = 1800

            for i in range(5):
                # now issue a couple background GBM jobs that we'll kill
                jobids = []     
                for j in range(5):
                    # FIX! apparently we can't reuse a model key after a cancel
                    kwargs['destination_key'] = 'GBMBad' + str(j)
                    # rjson error in poll_url: Job was cancelled by user!
                    GBMFirstResult = h2o_cmd.runGBM(parseResult=parseResult, noPoll=True, **kwargs)
                    jobids.append(GBMFirstResult['job_key'])
                    h2o.check_sandbox_for_errors()

                    # try ray's 'models' request to see if anything blows up
                    modelsParams = {
                        'key': None,
                        'find_compatible_frames': 0,
                        'score_frame': None
                    }
                    modelsResult = h2o.nodes[0].models(timeoutSecs=10, **modelsParams)
                    print "modelsResult:", h2o.dump_json(modelsResult)

                    
                # have to pass the job id
                # for j in jobids:
                #     h2o.nodes[0].jobs_cancel(key=j)

                h2o_jobs.cancelAllJobs()
                # PUB-361. going to wait after cancel before reusing keys
                time.sleep(3)
                # am I getting a subsequent parse job cancelled?
                h2o_jobs.showAllJobs()

            if DELETE_KEYS:
                h2i.delete_keys_from_import_result(pattern=csvFilename, importResult=importResult)

if __name__ == '__main__':
    h2o.unit_main()
