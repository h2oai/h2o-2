import unittest
import random, sys, time, os
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import as h2i, h2o_exec as h2e

zeroList = []
for i in range(5):
    zeroList.append('ColumnRes' + str(i) + ' = 0')
    zeroList.append('ScalarRes' + str(i) + ' = 0')
    zeroList.append('MatrixRes' + str(i) + ' = 0')

# FIX! put these in 3?
# 'randomBitVector'
# 'randomFilter'
# 'log"
# do we have to restrict ourselves?
# 'makeEnum'
#        'MatrixRes<n> = slice(<keyX>[<row>],123)',
exprList = [
        'ColumnRes<n> = <keyX>[<col1>] + <keyX>[<col2>] + <keyX>[2]' ,
        'MatrixRes<n> = colSwap(<keyX>,<col1>,(<keyX>[2]==0 ? 54321 : 54321))',
        'ColumnRes<n> = <keyX>[<col1>]',
        'ScalarRes<n> = log(<keyX>[<col1>]) + ColumnRes<n>[0]',
        'ScalarRes<n> = min(<keyX>[<col1>])',
        'ScalarRes<n> = max(<keyX>[<col1>]) + ColumnRes<n-1>[0]',
        'ScalarRes<n> = mean(<keyX>[<col1>]) + ColumnRes<n-1>[0]',
        'ScalarRes<n> = sum(<keyX>[<col1>]) + ScalarRes0',
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
                ("covtype.data", "cA", 5),
            ]
        else:
            csvFilenameAll = [
                ("covtype.data", "cA", 5),
                ("covtype20x.data", "cD", 50),
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
            print "Parse result['desination_key']:", parseKey['destination_key']
            inspect = h2o_cmd.runInspect(None, parseKey['destination_key'])

            print "\n" + csvFilename
            h2e.exec_zero_list(zeroList)
            # we use colX+1 so keep it to 53
            h2e.exec_expr_list_rand(lenNodes, exprList, key2, 
                maxCol=53, maxRow=400000, maxTrials=200, timeoutSecs=timeoutSecs)


if __name__ == '__main__':
    h2o.unit_main()
