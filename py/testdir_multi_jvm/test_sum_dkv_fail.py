import unittest, random, sys, time
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import as h2i, h2o_exec as h2e

zeroList = [
        'Result0 = 0',
]

# the first column should use this
exprList = [
        # 'Result<n> = sum(<keyX>[<col1>])',
        # all sum to 179 if you use this. used to fail?
        'Result<n> = sum(<keyX>[21])',
    ]

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED, localhost
        SEED = h2o.setup_random_seed()
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(2)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        # wait while I inspect things
        # time.sleep(1500)
        h2o.tear_down_cloud()

    def test_sum_import_hosts(self):
        # make the timeout variable per dataset. it can be 10 secs for covtype 20x (col key creation)
        # so probably 10x that for covtype200
        #    ("covtype20x.data", "cD", 50, 20),
        #    ("covtype200x.data", "cE", 50, 200),
        csvFilenameAll = [
            ("covtype.data", "cA", 5,  1),
            ("covtype.data", "cB", 5,  1),
            ("covtype.data", "cC", 5,  1),
        ]

        ### csvFilenameList = random.sample(csvFilenameAll,1)
        csvFilenameList = csvFilenameAll
        ## h2b.browseTheCloud()
        lenNodes = len(h2o.nodes)
        importFolderPath = "standard"

        firstDone = False
        for (csvFilename, hex_key, timeoutSecs, resultMult) in csvFilenameList:
            # have to import each time, because h2o deletes source after parse
            csvPathname = importFolderPath + "/" + csvFilename
            # creates csvFilename.hex from file in importFolder dir 
            parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, 
                hex_key=hex_key, timeoutSecs=2000)
            print csvFilename, 'parse time:', parseResult['response']['time']
            print "Parse result['destination_key']:", parseResult['destination_key']

            # We should be able to see the parse result?
            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])

            print "\n" + csvFilename
            h2e.exec_zero_list(zeroList)
            colResultList = h2e.exec_expr_list_across_cols(lenNodes, exprList, hex_key, 
                minCol=0, maxCol=54, timeoutSecs=timeoutSecs)
            print "\ncolResultList", colResultList

            if not firstDone:
                colResultList0 = list(colResultList)
                good = [float(x) for x in colResultList0] 
                firstDone = True
            else:
                print "\n", colResultList0, "\n", colResultList
                # create the expected answer...i.e. N * first
                compare = [float(x)/resultMult for x in colResultList] 
                print "\n", good, "\n", compare
                self.assertEqual(good, compare, 'compare is not equal to good (first try * resultMult)')
        

if __name__ == '__main__':
    h2o.unit_main()
