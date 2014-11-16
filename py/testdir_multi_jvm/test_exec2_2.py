import unittest
import random, sys, time, os
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i, h2o_exec as h2e

zeroList = []
for i in range(5):
    zeroList.append('ColumnRes' + str(i) + ' = c(0)')
    zeroList.append('ScalarRes' + str(i) + ' = c(0)')
    zeroList.append('MatrixRes' + str(i) + ' = c(0)')

# FIX! put these in 3?
# 'randomBitVector'
# 'randomFilter'
# 'log"
# do we have to restrict ourselves?
# 'makeEnum'
#        'MatrixRes<n> = slice(<keyX>[<row>],123)',
exprList = [
        # this will fail if you index with col 0
        '<keyX>[,<col1>] = <keyX>[,<col1>]', 
        '<keyX>[,<col1>] = <keyX>[,<col1>] + <keyX>[,<col1>]',
        # three doesn't work ??
        # '<keyX>[,<col1>] = <keyX>[,<col1>] + <keyX>[,<col1>] + <keyX>[,<col1>]' ,
        '<keyX>[,<col1>] = <keyX>[,<col1>] + <keyX>[,<col2>]',
        '<keyX>[,<col1>] = <keyX>[,<col2>]==1',
        '<keyX>[,<col1>] = <keyX>[,<col2>]==1.0',
        '<keyX>[,<col1>] = <keyX>[,<col2>]>1.0',
        # FIX! have to update for exec2
        # 'MatrixRes<n> = colSwap(<keyX>,<col1>,(<keyX>[2]==0 ? 54321 : 54321))',
        # 'ColumnRes<n> = <keyX>[<col1>]',
        # 'ScalarRes<n> = log(<keyX>[<col1>]) + ColumnRes<n>[0]',
        # 'ScalarRes<n> = min(<keyX>[<col1>])',
        # 'ScalarRes<n> = max(<keyX>[<col1>]) + ColumnRes<n-1>[0]',
        # 'ScalarRes<n> = mean(<keyX>[<col1>]) + ColumnRes<n-1>[0]',
        # 'ScalarRes<n> = sum(<keyX>[<col1>]) + ScalarRes0',
    ]

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init(3,java_heap_GB=4)

    @classmethod
    def tearDownClass(cls):
        # wait while I inspect things
        # time.sleep(1500)
        h2o.tear_down_cloud()

    def test_exec_2(self):
        # exec2 doesn't autoframe? fvec everything
        # make the timeout variable per dataset. it can be 10 secs for covtype 20x (col key creation)
        # so probably 10x that for covtype200
        if h2o.localhost:
            maxTrials = 200
            csvFilenameAll = [
                ("covtype.data", "cA.hex", 15),
            ]
        else:
            maxTrials = 20
            csvFilenameAll = [
                ("covtype.data", "cA.hex", 15),
                ("covtype20x.data", "cA.hex", 60),
            ]

        ### csvFilenameList = random.sample(csvFilenameAll,1)
        csvFilenameList = csvFilenameAll
        ## h2b.browseTheCloud()
        lenNodes = len(h2o.nodes)
        importFolderPath = "standard"

        for (csvFilename, hex_key, timeoutSecs) in csvFilenameList:
            SEEDPERFILE = random.randint(0, sys.maxint)
            # creates csvFilename.hex from file in importFolder dir 
            csvPathname = importFolderPath + "/" + csvFilename
            parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, 
                hex_key=hex_key, timeoutSecs=2000)
            print "Parse result['desination_key']:", parseResult['destination_key']
            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])

            print "\n" + csvFilename
            h2e.exec_zero_list(zeroList)
            # we use colX+1 so keep it to 53
            h2e.exec_expr_list_rand(lenNodes, exprList, hex_key, 
                maxCol=53, maxRow=400000, maxTrials=maxTrials, timeoutSecs=timeoutSecs)

if __name__ == '__main__':
    h2o.unit_main()
