import unittest, time, sys, random
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i, h2o_jobs, h2o_gbm

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

    def test_GBM_with_cancels(self):
        print "do import/parse with VA"

        importFolderPath = 'standard'
        timeoutSecs = 500
        csvFilenameAll = [
            # have to use col name for response?
            # ("manyfiles-nflx-gz", "file_1.dat.gz", 378),
            # ("manyfiles-nflx-gz", "file_1.dat.gz", 378),
            # ("manyfiles-nflx-gz", "file_[1-9].dat.gz", 378),
            ("standard", "covtype.data", 54),
            # ("standard", "covtype20x.data", 54),
            ]
        # csvFilenameList = random.sample(csvFilenameAll,1)
        csvFilenameList = csvFilenameAll

        # pop open a browser on the cloud
        # h2b.browseTheCloud()

        for (importFolderPath, csvFilename, response) in csvFilenameList:
            # creates csvFilename.hex from file in importFolder dir 
            csvPathname = importFolderPath + "/" + csvFilename 
            

            (importResult, importPattern) = h2i.import_only(bucket='home-0xdiag-datasets', path=csvPathname, schema='local', timeoutSecs=50)
            parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, schema='local', hex_key='c.hex', 
                timeoutSecs=500, doSummary=False)

            h2o.check_sandbox_for_errors()
            print "\nparseResult", h2o.dump_json(parseResult)

            print "Parse result['destination_key']:", parseResult['destination_key']
            ## What's wrong here? too big?
            ### inspect = h2o_cmd.runInspect(key=parseResult['destination_key'], timeoutSecs=30, verbose=True)

            h2o.check_sandbox_for_errors()

            # have to avoid this on nflx data. colswap with exec
            # Exception: rjson error in gbm: Argument 'response' error: Only integer or enum/factor columns can be classified

            if importFolderPath=='manyfiles-nflx-gz':
                if DO_CLASSIFICATION:
                    # need to flip the right col! (R wise)
                    execExpr = 'c.hex[,%s]=c.hex[,%s]>15' % (response+1,response+1)
                    kwargs = { 'str': execExpr }
                    resultExec = h2o_cmd.runExec(**kwargs)

                # lets look at the response column now
                s = h2o_cmd.runSummary(key="c.hex", cols=response, max_ncols=1)
                # x = range(542)
                # remove the output too! (378)
                xIgnore = []
                # BUG if you add unsorted 378 to end. remove for now
                for i in [3, 4, 5, 6, 7, 8, 9, 10, 11, 14, 16, 17, 18, 19, 20, 424, 425, 426, 540, 541, response]:
                    # have to add 1 for col start with 1, now. plus the C
                    xIgnore.append("C" + str(i+1))
            else:
                # leave one col ignored, just to see?
                xIgnore = 'C1'

            modelKey = "GBMGood"
            params = {
                'destination_key': modelKey,
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
            start = time.time()
            GBMFirstResult = h2o_cmd.runGBM(parseResult=parseResult, noPoll=True, **kwargs)
            print "\nGBMFirstResult:", h2o.dump_json(GBMFirstResult)
            # no pattern waits for all

            for i in range(3):
                # now issue a couple background GBM jobs that we'll kill
                jobids = []     
                for j in range(5):
                    # FIX! apparently we can't reuse a model key after a cancel
                    kwargs['destination_key'] = 'GBMBad' + str(i) + str(j)
                    GBMFirstResult = h2o_cmd.runGBM(parseResult=parseResult, noPoll=True, **kwargs)
                    jobids.append(GBMFirstResult['job_key'])

                # have to pass the job id
                for j in jobids:
                    h2o.nodes[0].jobs_cancel(key=j)


            h2o_jobs.pollWaitJobs(pattern='GBMGood', timeoutSecs=300, pollTimeoutSecs=10, retryDelaySecs=5)
            elapsed = time.time() - start
            print "GBM training completed in", elapsed, "seconds."

            gbmTrainView = h2o_cmd.runGBMView(model_key=modelKey)
            # errrs from end of list? is that the last tree?
            errsLast = gbmTrainView['gbm_model']['errs'][-1]

            print "GBM 'errsLast'", errsLast
            if DO_CLASSIFICATION:
                cm = gbmTrainView['gbm_model']['cms'][-1]['_arr'] # use the last one
                pctWrongTrain = h2o_gbm.pp_cm_summary(cm);
                print "Last line of this cm might be NAs, not CM"
                print "\nTrain\n==========\n"
                print h2o_gbm.pp_cm(cm)
            else:
                print "GBMTrainView:", h2o.dump_json(gbmTrainView['gbm_model']['errs'])

            h2o.check_sandbox_for_errors()

            if DELETE_KEYS:
                h2i.delete_keys_from_import_result(pattern=csvFilename, importResult=importResult)

if __name__ == '__main__':
    h2o.unit_main()
