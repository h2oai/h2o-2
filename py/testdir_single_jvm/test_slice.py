import unittest, random, sys, time, os
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import2 as h2i, h2o_exec as h2e

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
exprList = [
        'MatrixRes<n> = slice(<hex_keyX>[<col1>],<row>)',
        # last param should be optional
        'MatrixRes<n> = slice(<hex_keyX>[<col1>],1)',
        'MatrixRes<n> = slice(<hex_keyX>[<col1>],2)',
        'MatrixRes<n> = slice(<hex_keyX>[<col1>],<row>,123)',
        'MatrixRes<n> = slice(<hex_keyX>[<col1>],<row>,1)',
        'ColumnRes<n> = <hex_keyX>[<col1>] + <hex_keyX>[<col2>] + <hex_keyX>[2]',
        'ColumnRes<n> = colSwap(<hex_keyX>,<col1>,(<hex_keyX>[2]==0 ? 54321 : 54321))',
        'ColumnRes<n> = <hex_keyX>[<col1>]',
        'ScalarRes<n> = sum(<hex_keyX>[<col1>]) + ScalarRes0',
    ]
exprErrorCaseList = [
        # error case: more rows than in dataset?
        'MatrixRes<n> = slice(<hex_keyX>[<col1>],1,1000000)',
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
            h2o.build_cloud(1)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_slice(self):
        importFolderPath = "standard"
        csvFilenameAll = [
            ("covtype.data", "cA", 5),
        ]

        ### csvFilenameList = random.sample(csvFilenameAll,1)
        csvFilenameList = csvFilenameAll
        lenNodes = len(h2o.nodes)
        for (csvFilename, hex_key2, timeoutSecs) in csvFilenameList:
            # creates csvFilename.hex from file in importFolder dir 
            parseResult = h2i.import_parse(bucket='standard', path=csvFilenamea, schema='put',
                hex_key2=hex_key2, timeoutSecs=2000)
            print csvFilename, 'parse time:', parseResult['response']['time']
            print "Parse result['desination_hex_key']:", parseResult['destination_hex_key']
            inspect = h2o_cmd.runInspect(None, parseResult['destination_hex_key'])

            print "\n" + csvFilename
            h2e.exec_zero_list(zeroList)
            # try the error case list
            # I suppose we should test the expected error is correct. 
            # Right now just make sure things don't blow up
            h2e.exec_expr_list_rand(lenNodes, exprErrorCaseList, hex_key2, 
                maxCol=53, maxRow=400000, maxTrials=5, 
                timeoutSecs=timeoutSecs, ignoreH2oError=True)
            # we use colX+1 so keep it to 53
            h2e.exec_expr_list_rand(lenNodes, exprList, hex_key2, 
                maxCol=53, maxRow=400000, maxTrials=100, timeoutSecs=timeoutSecs)

if __name__ == '__main__':
    h2o.unit_main()
