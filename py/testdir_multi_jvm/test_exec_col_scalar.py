import unittest
import random, sys, time, os
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import as h2i
import h2o_exec as h2e

zeroList = [
        'ScalarRes0 = 0',
        'ScalarRes1 = 0',
        'ScalarRes2 = 0',
        'ScalarRes3 = 0',
        # FIX! how can this work? no size specified??, so scalar?
        'ColumnRes0 = 0',
        'ColumnRes1 = 0',
        'ColumnRes2 = 0',
        'ColumnRes3 = 0',
        ]

# 'randomBitVector'
# 'randomFilter'
# 'log"
# 'makeEnum'
# bug?
# 'MatrixRes<n> = slice(<keyX>[<col1>],<row>)',
# 'MatrixRes<n> = colSwap(<keyX>,<col1>,(<keyX>[2]==0 ? 54321 : 54321))',
exprList = [
        'ColumnRes<n> = <keyX>[<col1>] + ColumnRes0',
        'ColumnRes<n> = <keyX>[<col1>] + ColumnRes1',
        'ColumnRes<n> = <keyX>[<col1>] + ColumnRes2',
        'ColumnRes<n> = <keyX>[<col1>] + ColumnRes3',
        'ScalarRes<n> = min(<keyX>[<col1>]) + ScalarRes0',
        'ScalarRes<n> = min(<keyX>[<col1>]) + ScalarRes1',
        'ScalarRes<n> = min(<keyX>[<col1>]) + ScalarRes2',
        'ScalarRes<n> = min(<keyX>[<col1>]) + ScalarRes3',
        'ColumnRes<n> = <keyX>[<col1>] + ColumnRes<n-1>',
        'ColumnRes<n> = <keyX>[<col1>] + ColumnRes<n-1>',
        'ColumnRes<n> = <keyX>[<col1>] + ColumnRes<n-1>',
        'ColumnRes<n> = <keyX>[<col1>] + ColumnRes<n-1>',
        'ScalarRes<n> = min(<keyX>[<col1>]) + ScalarRes<n-1>',
        'ScalarRes<n> = min(<keyX>[<col1>]) + ScalarRes<n-1>',
        'ScalarRes<n> = min(<keyX>[<col1>]) + ScalarRes<n-1>',
        'ScalarRes<n> = min(<keyX>[<col1>]) + ScalarRes<n-1>',
    ]

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        # random.seed(SEED)
        SEED = random.randint(0, sys.maxint)
        # if you have to force to redo a test
        # SEED = 
        random.seed(SEED)
        print "\nUsing random seed:", SEED
        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(3,java_heap_GB=4)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        # wait while I inspect things
        # time.sleep(1500)
        h2o.tear_down_cloud()

    def test_exec_import_hosts(self):
        # just do the import folder once
        # importFolderPath = "/home/hduser/hdfs_datasets"
        importFolderPath = "/home/0xdiag/datasets"
        h2i.setupImportFolder(None, importFolderPath)

        # make the timeout variable per dataset. it can be 10 secs for covtype 20x (col key creation)
        # so probably 10x that for covtype200
        if localhost:
            csvFilenameAll = [
                ("covtype.data", "cA", 15),
                ("covtype.data", "cB", 15),
            ]
        else:
            csvFilenameAll = [
                ("covtype.data", "cA", 15),
                ("covtype.data", "cB", 15),
                ("covtype20x.data", "cC", 60),
                ("covtype20x.data", "cD", 60),
            ]

        ### csvFilenameList = random.sample(csvFilenameAll,1)
        csvFilenameList = csvFilenameAll
        h2b.browseTheCloud()
        lenNodes = len(h2o.nodes)

        for (csvFilename, key2, timeoutSecs) in csvFilenameList:
            SEEDPERFILE = random.randint(0, sys.maxint)
            # creates csvFilename.hex from file in importFolder dir 
            parseKey = h2i.parseImportFolderFile(None, csvFilename, importFolderPath, 
                key2=key2, timeoutSecs=2000)
            print csvFilename, 'parse time:', parseKey['response']['time']
            print "Parse result['destination_key']:", parseKey['destination_key']
            inspect = h2o_cmd.runInspect(None, parseKey['destination_key'])

            print "\n" + csvFilename
            h2e.exec_zero_list(zeroList)
            h2e.exec_expr_list_rand(lenNodes, exprList, key2, 
                maxCol=54, maxRow=400000, maxTrials=200, timeoutSecs=timeoutSecs)

if __name__ == '__main__':
    h2o.unit_main()
