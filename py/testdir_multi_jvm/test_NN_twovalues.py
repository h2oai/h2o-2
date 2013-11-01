import unittest, time, sys, re
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_glm, h2o_browse as h2b, h2o_import as h2i, h2o_jobs

def write_syn_dataset(csvPathname, rowCount, rowDataTrue, rowDataFalse, outputTrue, outputFalse):
    dsf = open(csvPathname, "w+")
    for i in range(int(rowCount/2)):
        dsf.write(rowDataTrue + ',' + outputTrue + "\n")

    for i in range(int(rowCount/2)):
        dsf.write(rowDataFalse + ',' + outputFalse + "\n")
    dsf.close()

class GLM_twovalues(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        # fails with 3
        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(3, java_heap_GB=4)
        else:
            h2o_hosts.build_cloud_with_hosts(1)
        # h2b.browseTheCloud()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud(h2o.nodes)
    
    def test_GLM_twovalues(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        csvFilename = "syn_twovalues.csv"
        csvPathname = SYNDATASETS_DIR + '/' + csvFilename

        rowDataTrue    = "1, 0, 65, 1, 2, 1, 1, 4, 1, 4, 1, 4"
        rowDataFalse   = "0, 1, 0, -1, -2, -1, -1, -4, -1, -4, -1, -4" 

        twoValueList = [
            ('A','B',0, 14),
            ('A','B',1, 14),
            (0,1,0, 12),
            (0,1,1, 12),
            (0,1,'NaN', 12),
            (1,0,'NaN', 12),
            (-1,1,0, 12),
            (-1,1,1, 12),
            (-1e1,1e1,1e1, 12),
            (-1e1,1e1,-1e1, 12),
            ]

        trial = 0
        for (outputTrue, outputFalse, case, coeffNum) in twoValueList:
            write_syn_dataset(csvPathname, 20, 
                rowDataTrue, rowDataFalse, str(outputTrue), str(outputFalse))

            start = time.time()
            hex_key = csvFilename + "_" + str(trial)
            kwargs = {'case': case, 'y': 12, 'family': 'binomial', 'alpha': 0, 'beta_epsilon': 0.0002}

            # default takes 39 iterations? play with alpha/beta
            parseResult = h2i.import_parse(path=csvPathname, schema='put', hex_key=hex_key)
            print "using outputTrue: %s outputFalse: %s" % (outputTrue, outputFalse)


            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
            print "\n" + csvPathname, \
                "    num_rows:", "{:,}".format(inspect['num_rows']), \
                "    num_cols:", "{:,}".format(inspect['num_cols'])

            response = inspect['num_cols'] - 1
            # up to but not including
            x = ",".join(map(str,range(response)))

            kwargs = {
                # this is ignore??
                'response': response,
                'cols': x, # apparently no longer required? 
                'ignored_cols': None, # this is not consistent with ignored_cols_by_name
                'classification': 1,
                'validation': hex_key,
                'activation': 'Tanh', # 'Rectifier'
                'hidden': 500, # comma separated values, or from:to:step
                'rate': 0.01,  # learning rate
                'l2': 1.0E-4, # regularization
                'epochs': 2, # how many times dataset should be iterated
                'destination_key': 'a.hex',
            }

            for iteration in range(2):
                timeoutSecs = 600
                start = time.time()
                h2o.beta_features = True
                nnResult = h2o_cmd.runNNet(parseResult=parseResult, timeoutSecs=timeoutSecs, noPoll=True, **kwargs)
                h2o.beta_features = False

                print "Hack: neural net apparently doesn't support the right polling response yet?"
                h2o_jobs.pollWaitJobs(pattern=None, timeoutSecs=300, pollTimeoutSecs=10, retryDelaySecs=5)

                print "FIX! need to add something that looks at the neural net result here?"
                print "nnResult:", h2o.dump_json(nnResult)

                print "trial #", trial, "iteration #", iteration, "NN end on ", csvFilename, 'took', time.time() - start, 'seconds'
                # h2b.browseJsonHistoryAsUrlLastMatch("GLM")
                h2o.check_sandbox_for_errors()

            trial += 1

if __name__ == '__main__':
    h2o.unit_main()
