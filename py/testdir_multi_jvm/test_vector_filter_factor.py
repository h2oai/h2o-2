import unittest
import random, sys, time, os
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import as h2i, h2o_exec as h2e

zeroList = [
        'Result0 = 0',
        'Result1 = 0',
]

exprList = [
        'Result<n> = <keyX>[<col1>] + <keyX>[<col2>] + <keyX>[2]' ,

# FIX! bug due to missing value in col 54
#        ['Result<n> = factor(<keyX>[54])',
        'Result<n> = factor(<keyX>[53])',
        'Result<n> = factor(<keyX>[28])',
        'Result<n> = randomBitVector(<row>,1,12345)',
        'Result<n> = randomBitVector(<row>,0,23456)',
# FIX! bugs in all of these?
#        'Result<n> = randomFilter(<keyX>[<col1>],<row>)',
#        'Result<n> = randomFilter(<keyX>,<row>)',
#        'Result<n> = randomFilter(<keyX>,3)',

        'Result<n> = <keyX>[<col1>]',
        'Result<n> = min(<keyX>[<col1>])',
        'Result<n> = max(<keyX>[<col1>]) + Result<n-1>',
        'Result<n> = sum(<keyX>[<col1>]) + Result.hex',
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
            # h2o.build_cloud(3,java_heap_GB=4)
            h2o.build_cloud(1,java_heap_GB=4)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        # wait while I inspect things
        # time.sleep(1500)
        h2o.tear_down_cloud()

    def test_vector_filter_factor(self):
        # just do the import folder once
        # importFolderPath = "/home/hduser/hdfs_datasets"
        importFolderPath = "/home/0xdiag/datasets"
        h2i.setupImportFolder(None, importFolderPath)

        # make the timeout variable per dataset. it can be 10 secs for covtype 20x (col key creation)
        # so probably 10x that for covtype200
        if localhost:
            maxTrials = 200
            csvFilenameAll = [
                ("covtype.data", "cA", 5),
                ("covtype.data", "cB", 5),
            ]
        else:
            maxTrials = 20
            csvFilenameAll = [
                ("covtype.data", "cA", 5),
                ("covtype20x.data", "cC", 50),
            ]

        ### csvFilenameList = random.sample(csvFilenameAll,1)
        csvFilenameList = csvFilenameAll
        h2b.browseTheCloud()
        lenNodes = len(h2o.nodes)

        for (csvFilename, key2, timeoutSecs) in csvFilenameList:
            # creates csvFilename.hex from file in importFolder dir 
            parseKey = h2i.parseImportFolderFile(None, csvFilename, importFolderPath, 
                key2=key2, timeoutSecs=2000)
            print csvFilename, 'parse time:', parseKey['response']['time']
            print "Parse result['destination_key']:", parseKey['destination_key']

            # We should be able to see the parse result?
            inspect = h2o_cmd.runInspect(None, parseKey['destination_key'])

            print "\n" + csvFilename
            h2e.exec_zero_list(zeroList)
            # does n+1 so use maxCol 53
            h2e.exec_expr_list_rand(lenNodes, exprList, key2, 
                maxCol=53, maxRow=400000, maxTrials=maxTrials, timeoutSecs=timeoutSecs)


if __name__ == '__main__':
    h2o.unit_main()
