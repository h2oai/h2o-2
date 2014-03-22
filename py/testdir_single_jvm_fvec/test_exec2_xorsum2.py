import unittest, random, sys, time
sys.path.extend(['.','..','py'])
import h2o, h2o_browse as h2b, h2o_exec as h2e, h2o_hosts, h2o_import as h2i, h2o_cmd, h2o_util

# new ...ability to reference cols
# src[ src$age<17 && src$zip=95120 && ... , ]
# can specify values for enums ..values are 0 thru n-1 for n enums

exprList = [
        'a=c(1); a = sum(r1[,1])',
        'h=c(1); h = xorsum(r1[,1])',
        ]

#********************************************************************************
def write_syn_dataset(csvPathname, rowCount, colCount, expectedMin, expectedMax, SEEDPERFILE):
    dsf = open(csvPathname, 'w')
    expectedRange = (expectedMax - expectedMin)
    expectedFpSum = 0.0
    for row in range(rowCount):
        rowData = []
        for j in range(colCount):
            value = expectedMin + (random.random() * expectedRange)
            if 1==1:
                # value = row * 2

                # bad sum
                # value = 5555555555555 + row
                # bad
                # value = 555555555555 + row
                # value = 55555555555 + row

                # fail
                # value = 5555555555 + row
                # exp = random.randint(0,120)
                # 50 bad?
                exp = random.randint(0,10)
                value = 3 * (2 ** exp) 

                # value = -1 * value
                # value = 2e9 + row
                # value = 3 * row
            r = random.randint(0,1)
            if False and r==0:
                value = -1 * value
            # hack

            # get the expected patterns from python
            fpResult = float(value)
            expectedUll = h2o_util.doubleToUnsignedLongLong(fpResult)
            expectedFpSum += fpResult
            # print "%30s" % "expectedUll (0.16x):", "0x%0.16x" % expectedUll

            # Now that you know how many decimals you want, 
            # say, 15, just use a rstrip("0") to get rid of the unnecessary 0s:
            # s = ("%.16e" % value).rstrip("0")
            s = ("%.16e" % value)
            rowData.append(s)

        rowDataCsv = ",".join(map(str,rowData))
        dsf.write(rowDataCsv + "\n")

    dsf.close()
    return (expectedUll, expectedFpSum)

#********************************************************************************
class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED, localhost
        SEED = h2o.setup_random_seed()
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(1, java_heap_GB=28)
        else:
            h2o_hosts.build_cloud_with_hosts(1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_exec2_xorsum(self):
        h2o.beta_features = True
        SYNDATASETS_DIR = h2o.make_syn_dir()

        tryList = [
            (10000, 1, 'r1', 0, 10, None),
        ]

        ullResultList = []
        for (rowCount, colCount, hex_key, expectedMin, expectedMax, expected) in tryList:
            SEEDPERFILE = random.randint(0, sys.maxint)
            # dynamic range of the data may be useful for estimating error
            maxDelta = expectedMax - expectedMin

            csvFilename = 'syn_real_' + str(rowCount) + 'x' + str(colCount) + '.csv'
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename
            csvPathnameFull = h2i.find_folder_and_filename(None, csvPathname, returnFullPath=True)
            print "Creating random", csvPathname
            (expectedUll, expectedFpSum)  = write_syn_dataset(csvPathname, 
                rowCount, colCount, expectedMin, expectedMax, SEEDPERFILE)

            parseResult = h2i.import_parse(path=csvPathname, schema='local', hex_key=hex_key, 
                timeoutSecs=3000, retryDelaySecs=2)
            inspect = h2o_cmd.runInspect(key=hex_key)
            print "numRows:", inspect['numRows']
            print "numCols:", inspect['numCols']
            inspect = h2o_cmd.runInspect(key=hex_key, offset=-1)
            print "inspect offset = -1:", h2o.dump_json(inspect)

            
            # looking at the 8 bytes of bits for the h2o doubles
            # xorsum will zero out the sign and exponent
            for execExpr in exprList:
                start = time.time()
                (execResult, fpResult) = h2e.exec_expr(h2o.nodes[0], execExpr, 
                    resultKey=None, timeoutSecs=300)
                print 'exec took', time.time() - start, 'seconds'
                print "execResult:", h2o.dump_json(execResult)
                print ""
                print "%30s" % "fpResult:", "%.15f" % fpResult
                ullResult = h2o_util.doubleToUnsignedLongLong(fpResult)
                print "%30s" % "bitResult (0.16x):", "0x%0.16x" % ullResult
                print "%30s" % "expectedUll (0.16x):", "0x%0.16x" % expectedUll
                # print "%30s" % "hex(bitResult):", hex(ullResult)
                ullResultList.append((ullResult, fpResult))

            h2o.check_sandbox_for_errors()

            print "first result was from a sum. others are xorsum"
            print "ullResultList:"
            for ullResult, fpResult in ullResultList:
                print "%30s" % "ullResult (0.16x):", "0x%0.16x   %s" % (ullResult, fpResult)
            expectedUllAsDouble = h2o_util.unsignedLongLongToDouble(expectedUll)
            print "%30s" % "expectedUll (0.16x):", "0x%0.16x   %s" % (expectedUll, expectedUllAsDouble)
            expectedFpSumAsLongLong = h2o_util.doubleToUnsignedLongLong(expectedFpSum)
            print "%30s" % "expectedFpSum (0.16x):", "0x%0.16x   %s" % (expectedFpSumAsLongLong, expectedFpSum)

if __name__ == '__main__':
    h2o.unit_main()
