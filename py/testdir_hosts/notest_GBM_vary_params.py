import unittest, os, csv, random, sys, time, re, itertools
from pprint import pprint
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i, h2o_glm, h2o_util, h2o_rf,h2o_jobs as h2j

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

    def test_GBM_parseTrain(self):
        #folderpath, filename, keyname, timeout
        bucket = 'home-0xdiag-datasets'
        
        files = [('mnist', 'mnist_training.csv.gz', 'mnistsmalltrain.hex',1800,0)
                ]

        grid = [[1,10,100,1000], [0.0,0.01,0.001,0.0001,1], [1,2], [1,10,100]] 
        grid = list(itertools.product(*grid))
        grid = random.sample(grid, 10) #don't do all 120, but get a random sample
        for importFolderPath,csvFilename,trainKey,timeoutSecs,response in files:
            # PARSE train****************************************
            start = time.time()
            parseResult = h2i.import_parse(bucket=bucket, path=importFolderPath + "/" + csvFilename, schema='local',
                hex_key=trainKey, timeoutSecs=timeoutSecs)
            elapsed = time.time() - start
            print "parse end on ", csvFilename, 'took', elapsed, 'seconds',\
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
            print "parse result:", parseResult['destination_key']
            csv_header = ('nJVMs','java_heap_GB', 'dataset', 'ntrees', 'max_depth', 'learn_rate', 'min_rows','trainTime')
            for ntree, learn_rate, max_depth, min_rows in grid:
                if not os.path.exists('gbm_grid.csv'):
                    output = open('gbm_grid.csv', 'w')
                    output.write(','.join(csv_header)+'\n')
                else:
                    output = open('gbm_grid.csv', 'a')

                csvWrt = csv.DictWriter(output, fieldnames=csv_header, restval=None,
                        dialect='excel', extrasaction='ignore',delimiter=',')
                java_heap_GB = h2o.nodes[0].java_heap_GB

                params = {
                 'destination_key': 'GBMKEY',
                 'learn_rate': learn_rate,
                 'ntrees':ntree,
                 'max_depth':max_depth,
                 'min_rows':min_rows,
                 'response':response
                }
                print "Using these parameters for GBM: ", params
                kwargs = params.copy()
                #noPoll -> False when GBM finished
                start = time.time()
                GBMResult = h2o_cmd.runGBM(parseResult=parseResult,noPoll=True,timeoutSecs=timeoutSecs,**kwargs)
                h2j.pollWaitJobs(pattern="GBMKEY",timeoutSecs=3600,pollTimeoutSecs=3600)
                #print "GBM training completed in", GBMResult['python_elapsed'], "seconds.", \
                #    "%f pct. of timeout" % (GBMResult['python_%timeout'])
                #print GBMResult
                GBMView = h2o_cmd.runGBMView(model_key='GBMKEY')
                print GBMView['gbm_model']['errs']
                elapsed = time.time() - start
                row = {'nJVMs':len(h2o.nodes),'java_heap_GB':java_heap_GB,'dataset':'mnist_training.csv.gz',
                       'learn_rate':learn_rate,'ntrees':ntree,'max_depth':max_depth,
                       'min_rows':min_rows, 'trainTime':elapsed}
                print row
                csvWrt.writerow(row)

if __name__ == '__main__':
    h2o.unit_main()
