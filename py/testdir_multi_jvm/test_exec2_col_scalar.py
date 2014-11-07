import unittest, random, sys, time, os
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i, h2o_exec as h2e

zeroList = [
        # this is an error case
        # 'ScalarRes0 = cA[0,0]',

        # shouldn't this create a key?
        # 'ScalarRes0 = cA[1,1]',
        'ScalarRes0 = c(cA[1,1])',
        'ScalarRes1 = c(cA[1,1])',
        'ScalarRes2 = c(cA[1,1])',
        'ScalarRes3 = c(cA[1,1])',
        # this is an error case
        # 'ColumnRes0 = cA[,0]',
        'ColumnRes0 = cA[,1]',
        'ColumnRes1 = cA[,1]',
        'ColumnRes2 = cA[,1]',
        'ColumnRes3 = cA[,1]',
        ]

# 'randomBitVector'
# 'randomFilter'
# 'log"
# 'makeEnum'
# bug?
# 'MatrixRes<n> = slice(<keyX>[<col1>],<row>)',
# 'MatrixRes<n> = colSwap(<keyX>,<col1>,(<keyX>[2]==0 ? 54321 : 54321))',
exprList = [
        'ColumnRes<n> = <keyX>[,<col1>] + ColumnRes0',
        'ColumnRes<n> = <keyX>[,<col1>] + ColumnRes1',
        'ColumnRes<n> = <keyX>[,<col1>] + ColumnRes2',
        'ColumnRes<n> = <keyX>[,<col1>] + ColumnRes3',
        'ScalarRes<n> = min(<keyX>[,<col1>]) + ScalarRes0',
        'ScalarRes<n> = min(<keyX>[,<col1>]) + ScalarRes1',
        'ScalarRes<n> = min(<keyX>[,<col1>]) + ScalarRes2',
        'ScalarRes<n> = min(<keyX>[,<col1>]) + ScalarRes3',
        'ColumnRes<n> = <keyX>[,<col1>] + ColumnRes<n-1>',
        'ColumnRes<n> = <keyX>[,<col1>] + ColumnRes<n-1>',
        'ColumnRes<n> = <keyX>[,<col1>] + ColumnRes<n-1>',
        'ColumnRes<n> = <keyX>[,<col1>] + ColumnRes<n-1>',
        'ScalarRes<n> = min(<keyX>[,<col1>]) + ScalarRes<n-1>',
        'ScalarRes<n> = min(<keyX>[,<col1>]) + ScalarRes<n-1>',
        'ScalarRes<n> = min(<keyX>[,<col1>]) + ScalarRes<n-1>',
        'ScalarRes<n> = min(<keyX>[,<col1>]) + ScalarRes<n-1>',
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

    def test_exec2_col_scalar(self):
        # make the timeout variable per dataset. it can be 10 secs for covtype 20x (col key creation)
        # so probably 10x that for covtype200
        maxTrials = 20
        csvFilenameAll = [
            ("covtype.data", 15),
            ("covtype20x.data", 60),
        ]

        ### csvFilenameList = random.sample(csvFilenameAll,1)
        csvFilenameList = csvFilenameAll
        ## h2b.browseTheCloud()
        lenNodes = len(h2o.nodes)
        importFolderPath = "standard"

        # just always use the same hex_key, so the zeroList is right all the time
        hex_key = 'cA'
        for (csvFilename, timeoutSecs) in csvFilenameList:
            SEEDPERFILE = random.randint(0, sys.maxint)
            # creates csvFilename.hex from file in importFolder dir 
            csvPathname = importFolderPath + "/" + csvFilename
            parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, 
                hex_key=hex_key, timeoutSecs=2000)
            print "Parse result['destination_key']:", parseResult['destination_key']
            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])

            print "\n" + csvFilename
            h2e.exec_zero_list(zeroList)
            h2e.exec_expr_list_rand(lenNodes, exprList, hex_key, 
                maxCol=54, maxRow=400000, maxTrials=maxTrials, timeoutSecs=timeoutSecs)

if __name__ == '__main__':
    h2o.unit_main()
