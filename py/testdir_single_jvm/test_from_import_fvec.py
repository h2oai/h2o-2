import unittest, time, sys
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd,h2o_hosts, h2o_browse as h2b, h2o_import as h2i, h2o_hosts, h2o_jobs
import time, random

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(1,java_heap_GB=10)
        else:
            h2o_hosts.build_cloud_with_hosts(1,java_heap_GB=10)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_from_import_fvec(self):

        print "Sets h2o.beat_features like -bf at command line"
        print "this will redirect import and parse to the 2 variants"
        h2o.beta_features = True

        importFolderPath = 'standard'
        timeoutSecs = 500
        csvFilenameAll = [
            # ("manyfiles-nflx-gz", "file_1.dat"),
            ("manyfiles-nflx-gz", "file_[1-9].dat.gz", 378),
            ("standard", "covtype.data", 54),
            ("standard", "covtype20x.data", 54),
            ]
        # csvFilenameList = random.sample(csvFilenameAll,1)
        csvFilenameList = csvFilenameAll

        # pop open a browser on the cloud
        # h2b.browseTheCloud()

        for (importFolderPath, csvFilename, vresponse) in csvFilenameList:
            # creates csvFilename.hex from file in importFolder dir 
            csvPathname = importFolderPath + "/" + csvFilename 
            
            h2o.beta_features = False

            (importResult, importPattern) = h2i.import_only(bucket='home-0xdiag-datasets', path=csvPathname, schema='local', timeoutSecs=50)
            parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, schema='local', hex_key='c.hex', 
                timeoutSecs=500, noPoll=True, doSummary=False) # can't do summary until parse result is correct json

            h2o.check_sandbox_for_errors()

            print "\nparseResult", h2o.dump_json(parseResult)

            # wait for it to show up in jobs?
            time.sleep(2)
            # no pattern waits for all
            h2o_jobs.pollWaitJobs(pattern=None, timeoutSecs=300, pollTimeoutSecs=10, retryDelaySecs=5)

            if not h2o.beta_features:
                print csvFilename, 'parse time:', parseResult['response']['time']
                print "Parse result['destination_key']:", parseResult['destination_key']
                inspect = h2o_cmd.runInspect(key=parseResult['destination_key'], timeoutSecs=30)

            h2o.beta_features = True
            inspect = h2o_cmd.runInspect(key='c.hex', timeoutSecs=30)

            h2o.check_sandbox_for_errors()

            # hack it because no response from Parse2
            parseResult = {'destination_key': 'c.hex'}

            # placeholder stuff from GLM
            # this is for the nflx datasets
            x = range(542) # don't include the output column
            # remove the output too! (378)
            for i in [3, 4, 5, 6, 7, 8, 9, 10, 11, 14, 16, 17, 18, 19, 20, 424, 425, 426, 540, 541, 378]:
                x.remove(i)
            x = ",".join(map(str,x))

            GLMkwargs = {'x': x, 'y': 378, 'case': 15, 'case_mode': '>',
                'max_iter': 10, 'n_folds': 1, 'alpha': 0.2, 'lambda': 1e-5}


            # have to avoid this on nflx data. colswap with exec
            # Exception: rjson error in gbm: Argument 'vresponse' error: Only integer or enum/factor columns can be classified
            if importFolderPath=='manyfiles-nflx-gz':
                execExpr = 'c.hex=colSwap(c.hex,378,(c.hex[378]>15 ? 1 : 0))'
                resultExec = h2o_cmd.runExec(expression=execExpr)

            params = {
                'destination_key': "GBMKEY",
                'learn_rate': .1,
                'ntrees': 1,
                'max_depth': 8,
                'min_rows': 1,
                'vresponse': vresponse
                }

            kwargs = params.copy()
            h2o.beta_features = True
            timeoutSecs = 1800
            #noPoll -> False when GBM finished
            GBMResult = h2o_cmd.runGBM(parseResult=parseResult, noPoll=True,**kwargs)
            print "GBM training completed in", GBMResult['python_elapsed'], "seconds.", \
                "%f pct. of timeout" % (GBMResult['python_%timeout'])

            # wait for it to show up in jobs?
            time.sleep(2)
            # no pattern waits for all
            h2o_jobs.pollWaitJobs(pattern=None, timeoutSecs=300, pollTimeoutSecs=10, retryDelaySecs=5)


            h2o.check_sandbox_for_errors()

            ## h2b.browseJsonHistoryAsUrlLastMatch("RFView")
            ## time.sleep(10)

            # just to make sure we test this
            # FIX! currently the importResult is empty for fvec
            if 1==0:
                h2i.delete_keys_from_import_result(pattern=csvFilename, importResult=importResult)

            sys.stdout.write('.')
            sys.stdout.flush() 

if __name__ == '__main__':
    h2o.unit_main()
