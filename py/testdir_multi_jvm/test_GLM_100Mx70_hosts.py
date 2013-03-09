import unittest
import random, sys, time, os
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import as h2i, h2o_glm

def write_syn_dataset(csvPathname, rowCount, colCount, SEED):
    r1 = random.Random(SEED)
    dsf = open(csvPathname, "w+")

    for i in range(rowCount):
        rowData = []
        for j in range(colCount):
            ri = r1.randint(0,1)
            rowData.append(ri)

        ri = r1.randint(0,1)
        rowData.append(ri)

        rowDataCsv = ",".join(map(str,rowData))
        dsf.write(rowDataCsv + "\n")

    dsf.close()


class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = random.randint(0, sys.maxint)
        # SEED = 
        random.seed(SEED)
        print "\nUsing random seed:", SEED
        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(1,java_heap_GB=28)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GLM_100Mx70_hosts(self):
        # enable this if you need to re-create the file
        if 1==0:
            SYNDATASETS_DIR = h2o.make_syn_dir()
            createList = [
                (100000000, 70, 'cA', 10000), 
                ]

            for (rowCount, colCount, key2, timeoutSecs) in createList:
                csvFilename = 'syn_' + str(SEEDPERFILE) + "_" + str(rowCount) + 'x' + str(colCount) + '.csv'
                csvPathname = SYNDATASETS_DIR + '/' + csvFilename
                print "Creating random", csvPathname
                SEEDPERFILE = random.randint(0, sys.maxint)
                write_syn_dataset(csvPathname, rowCount, colCount, SEEDPERFILE)
            # Have to copy it to /home/0xdiag/datasets!


        if localhost:
            csvFilenameList = [
                # ('rand_logreg_500Kx70.csv.gz', 500, 'rand_500Kx70'),
                # ('rand_logreg_1Mx70.csv.gz', 500, 'rand_1Mx70'),
                ('rand_logreg_100000000x70.csv.gz', 500, 'rand_100Mx70.hex'),
                ]
        else:
            # None is okay for key2
            csvFilenameList = [
                # ('rand_logreg_500Kx70.csv.gz', 500, 'rand_500Kx70'),
                # ('rand_logreg_1Mx70.csv.gz', 500, 'rand_1Mx70'),
                ('rand_logreg_100000000x70.csv.gz', 500, 'rand_100Mx70.hex'),
                ]

        ### h2b.browseTheCloud()
        lenNodes = len(h2o.nodes)

        importFolderPath = '/home/0xdiag/datasets'
        h2i.setupImportFolder(None, importFolderPath)
        for csvFilename, timeoutSecs, key2 in csvFilenameList:
            # creates csvFilename.hex from file in importFolder dir 
            parseKey = h2i.parseImportFolderFile(None, csvFilename, importFolderPath, key2=key2,
                timeoutSecs=2000, retryDelaySecs=5, initialDelaySecs=10, pollTimeoutSecs=60)
            inspect = h2o_cmd.runInspect(None, parseKey['destination_key'])
            csvPathname = importFolderPath + "/" + csvFilename
            num_rows = inspect['num_rows']
            num_cols = inspect['num_cols']

            print "\n" + csvPathname, \
                "    num_rows:", "{:,}".format(num_rows), \
                "    num_cols:", "{:,}".format(num_cols)

            y = num_cols - 1
            kwargs = {
                'family': 'binomial',
                'link': 'logit',
                'y': y, 
                'max_iter': 8, 
                'num_cross_validation_folds': 0, 
                'beta_eps': 1e-4,
                'alpha': 0, 
                'lambda': 0 
                }

            for trial in range(3):
                start = time.time()
                glm = h2o_cmd.runGLMOnly(parseKey=parseKey, timeoutSecs=timeoutSecs, **kwargs)
                elapsed = time.time() - start
                print "glm", trial, "end on ", csvPathname, 'took', elapsed, 'seconds.',
                print "%d pct. of timeout" % ((elapsed/timeoutSecs) * 100)

                h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)



if __name__ == '__main__':
    h2o.unit_main()
