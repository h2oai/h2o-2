import unittest
import random, sys, time, os
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import as h2i, h2o_exec as h2e

exprList = [
        # keep it less than 6 so we can see all the values with an inspect?
        (6,0,6, 'a.hex = randomBitVector(6,0,9)'),
        (6,1,5, 'a.hex = randomBitVector(6,1,9)'),
        (6,2,4, 'a.hex = randomBitVector(6,2,9)'),
        (6,3,3, 'a.hex = randomBitVector(6,3,9)'),
        (6,4,2, 'a.hex = randomBitVector(6,4,9)'),
        (6,5,1, 'a.hex = randomBitVector(6,5,9)'),
        (6,6,0, 'a.hex = randomBitVector(6,6,9)'),
    ]

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(1)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_randomBitVector(self):
        ### h2b.browseTheCloud()

        trial = 0
        for (expectedRows, expectedOnes, expectedZeroes, execExpr) in exprList:
            resultKey="Result.hex"
            execResultInspect, min_value = h2e.exec_expr(h2o.nodes[0], execExpr,
                resultKey=resultKey, timeoutSecs=4)

            num_cols = execResultInspect['num_cols']
            num_rows = execResultInspect['num_rows']
            row_size = execResultInspect['row_size']

            # FIX! is this right?
            if (num_cols != 1):
                raise Exception("Wrong num_cols in randomBitVector result.  expected: %d, actual: %d" %\
                    (1, num_cols))

            if (num_rows != expectedRows):
                raise Exception("Wrong num_rows in randomBitVector result.  expected: %d, actual: %d" %\
                    (expectedRows, num_rows))

            if (row_size != 1):
                raise Exception("Wrong row_size in randomBitVector result.  expected: %d, actual: %d" %\
                    (1, row_size))

            # count the zeroes and ones in the created data
            
            execResultDataInspect = h2o_cmd.runInspect(key=resultKey, offset=0)
            actualZeroes = 0
            actualOnes = 0
            
            rowData = execResultDataInspect["rows"]
            for i in range (expectedRows):
                # row_data = execResultInspect["rows"][0]
                # value = execResultInspect["rows"][0]["0"]
                value = rowData[i]["bits"]
                if value == 0:
                    actualZeroes += 1
                elif value == 1:
                    actualOnes += 1
                else:
                    raise Exception("Bad value in cols dict of randomBitVector result. key: %s, value: %s" % (i, value))

            if (actualOnes != expectedOnes):
                raise Exception("Wrong number of 1's in randomBitVector result.  expected: %d, actual: %d" %\
                    (expectedOnes, actualOnes))
                
            if (actualZeroes != expectedZeroes):
                raise Exception("Wrong number of 0's in randomBitVector result.  expected: %d, actual: %d" %\
                    (expectedZeroes, actualZeroes))

            ### h2b.browseJsonHistoryAsUrlLastMatch("Inspect")
            trial += 1


if __name__ == '__main__':
    h2o.unit_main()
