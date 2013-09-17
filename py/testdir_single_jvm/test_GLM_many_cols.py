import unittest, random, sys, time
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
        global SEED, localhost
        SEED = h2o.setup_random_seed()
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(1,java_heap_GB=10)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        ### time.sleep(3600)
        h2o.tear_down_cloud()

    def test_GLM_many_cols(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()

        if localhost:
            tryList = [
                (10000, 100, 'cA', 300), 
                (10000, 1000, 'cB', 300), 
                (10000, 3000, 'cC', 500), 
                ]
        else:
            tryList = [
                # (10000, 10, 'cB', 300), 
                # (10000, 50, 'cC', 300), 
                (10000, 100, 'cD', 300), 
                (10000, 200, 'cE', 300), 
                (10000, 300, 'cF', 300), 
                (10000, 400, 'cG', 300), 
                (10000, 500, 'cH', 300), 
                (10000, 1000, 'cI', 300), 
                ]

        ### h2b.browseTheCloud()
        lenNodes = len(h2o.nodes)

        for (rowCount, colCount, hex_key, timeoutSecs) in tryList:
            SEEDPERFILE = random.randint(0, sys.maxint)
            # csvFilename = 'syn_' + str(SEEDPERFILE) + "_" + str(rowCount) + 'x' + str(colCount) + '.csv'
            csvFilename = 'syn_' + "binary" + "_" + str(rowCount) + 'x' + str(colCount) + '.csv'
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename

            print "Creating random", csvPathname
            write_syn_dataset(csvPathname, rowCount, colCount, SEEDPERFILE)

            parseResult = h2i.import_parse(path=csvPathname, hex_key=hex_key, schema='put', timeoutSecs=10)
            print csvFilename, 'parse time:', parseResult['response']['time']
            print "Parse result['destination_key']:", parseResult['destination_key']

            # We should be able to see the parse result?
            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
            print "\n" + csvFilename

            y = colCount
            # normally we dno't create x and rely on the default
            # create the big concat'ed x like the browser, to see what happens
            x = ','.join(map(str, range(colCount)))
            kwargs = {'x': x, 'y': y, 'max_iter': 10, 'n_folds': 1, 'alpha': 0.2, 'lambda': 1e-5}

            start = time.time()
            glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=timeoutSecs, **kwargs)
            print "glm end on ", csvPathname, 'took', time.time() - start, 'seconds'
            h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)

            # if not h2o.browse_disable:
            #     h2b.browseJsonHistoryAsUrlLastMatch("Inspect")
            #     time.sleep(5)


if __name__ == '__main__':
    h2o.unit_main()
