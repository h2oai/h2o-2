import unittest, random, sys, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_browse as h2b, h2o_exec as h2e, h2o_import as h2i, h2o_cmd, h2o_util

# new ...ability to reference cols
# src[ src$age<17 && src$zip=95120 && ... , ]
# can specify values for enums ..values are 0 thru n-1 for n enums

exprList = [
    'h=c(1); h = xorsum(r1[,1])',
]

ROWS = 2000000
STOP_ON_ERROR = True
DO_BUG = True
DO_NEGATIVE = True
RANDOM_E_FP_FORMATS = True
CHUNKING_CNT = 100000
# subtracting hex numbers below and using this for max delta
ALLOWED_DELTA= 0x7fff

# interesting lsb diff in right format, leads to larger mantissa error on left
# mask -0x7ff from mantissa for mostly-right comparison (theoreticaly can flip lots of mantissa and exp)
# we actually do a sub and look at delta below (not mask) (I guess the hex gets interpreted differently than IEEE but that's okay)
# alright if we're mostly right..looking to catch catastrophic error in h2o

#      ullResult (0.16x): 0x01faad3090cdb9d8   3.98339643735e-299
# expectedUllSum (0.16x): 0x01faad3090cd88d7   3.98339643734e-299
#  expectedFpSum (0.16x): 0x48428c76f4d986cf   1.26235843623e+40

# http://babbage.cs.qc.cuny.edu/IEEE-754.old/64bit.html
# 10 digits of precision on the right..They both agree
# But the left is what the ieee fp representation is inside h2o (64-bit real)
# 
#  0x0d9435c98b2bc489   2.9598664472e-243
#  0x0d9435c98b2bd40b   2.9598664472e-243
# 
# 
# With more precision you can see the difference
# 
# hex: 0d9435c98b2bc489
# is: 2.9598664471952256e-243
# 
# hex: 0d9435c98b2bd40b
# is: 2.9598664471972913e-243

#********************************************************************************
def write_syn_dataset(csvPathname, rowCount, colCount, expectedMin, expectedMax, SEEDPERFILE, sel):
    # this only does the sum stuff for single cols right now
    if colCount!=1:
        raise Exception("only support colCount == 1 here right now %s", colCount)

    NUM_CASES = h2o_util.fp_format()
    if sel and (sel<0 or sel>=NUM_CASES):
        raise Exception("sel used to select from possible fp formats is out of range: %s %s", (sel, NUM_CASES))

    dsf = open(csvPathname, 'w')
    expectedRange = (expectedMax - expectedMin)
    expectedFpSum = float(0)
    expectedUllSum = int(0)
    for row in range(rowCount):
        rowData = []
        for j in range(colCount):

            # Be Nasty!. We know fp compression varies per chunk
            # so...adjust the random fp data, depending on what rows your are at
            # i.e. cluster results per chunk, smaller variance within chunk, larger variance outside of chunk

            # Actually: generate "different" data depending on where you are in the rows
            method = row % CHUNKING_CNT
            
            if method==1:
                value = expectedMin + (random.random() * expectedRange)
            elif method==2:
                value = random.randint(1,1e6)
            elif method==3:
                value = 5555555555555 + row
            else: # method == 0 and > 3

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

                # constrain the dynamic range of the numbers to be within IEEE-754 support
                # without loss of precision when adding. Why do we care though?
                # could h2o compress if values are outside that kind of dynamic range ?

                # we want a big exponent?
                # was
                # exp = random.randint(40,71)
                exp = random.randint(0,120)
                # skip over the current bug around int boundaries?
                # have a fixed base
                value = random.random() + (2 ** exp) 

                # value = -1 * value
                # value = 2e9 + row
                # value = 3 * row

            r = random.randint(0,4)
            # 20% negative
            if DO_NEGATIVE and r==0:
                value = -1 * value

            # print "%30s" % "expectedUllSum (0.16x):", "0x%0.16x" % expectedUllSum

            # Now that you know how many decimals you want, 
            # say, 15, just use a rstrip("0") to get rid of the unnecessary 0s:
            # old bugs was: can't rstrip if .16e is used because trailing +00 becomes +, causes NA

            # use a random fp format (string). use sel to force one you like

            # only keeps it to formats with "e"
            if RANDOM_E_FP_FORMATS:
                # s = h2o_util.fp_format(value, sel=sel) # this is e/f/g formats for a particular sel within each group
                # s = h2o_util.fp_format(value, sel=None) # this would be random
                s = h2o_util.fp_format(value, sel=None, only='e') # this would be random, within 'e' only
            else:
                s = h2o_util.fp_format(value, sel=sel, only='e') # use same format for all numbers

            # FIX! strip the trailing zeroes for now because they trigger a bug
            if DO_BUG:
                pass
            else:
                s = s.rstrip("0")

            # now our string formatting will lead to different values when we parse and use it 
            # so we move the expected value generation down here..i.e after we've formatted the string
            # we'll suck it back in as a fp number
            # get the expected patterns from python
            fpResult = float(s)
            expectedUllSum ^= h2o_util.doubleToUnsignedLongLong(fpResult)
            expectedFpSum += fpResult
            # s = ("%.16e" % value)
            rowData.append(s)

            rowDataCsv = ",".join(map(str,rowData))
            dsf.write(rowDataCsv + "\n")

    dsf.close()
    # zero the upper 4 bits of xorsum like h2o does to prevent inf/nan
    # print hex(~(0xf << 60))
    expectedUllSum &= (~(0xf << 60))
    return (expectedUllSum, expectedFpSum)

#********************************************************************************
class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init(java_heap_GB=4)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_exec2_xorsum2(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        tryList = [
            (ROWS, 1, 'r1', 0, 10, None),
        ]

        for trial in range(3):
            ullResultList = []
            NUM_FORMAT_CASES = h2o_util.fp_format()
            for (rowCount, colCount, hex_key, expectedMin, expectedMax, expected) in tryList:
                SEEDPERFILE = random.randint(0, sys.maxint)
                # dynamic range of the data may be useful for estimating error
                maxDelta = expectedMax - expectedMin

                csvFilename = 'syn_real_' + str(rowCount) + 'x' + str(colCount) + '.csv'
                csvPathname = SYNDATASETS_DIR + '/' + csvFilename
                csvPathnameFull = h2i.find_folder_and_filename(None, csvPathname, returnFullPath=True)
                print "Creating random", csvPathname

                sel = random.randint(0, NUM_FORMAT_CASES-1)
                (expectedUllSum, expectedFpSum)  = write_syn_dataset(csvPathname, 
                    rowCount, colCount, expectedMin, expectedMax, SEEDPERFILE, sel)
                expectedUllSumAsDouble = h2o_util.unsignedLongLongToDouble(expectedUllSum)
                expectedFpSumAsLongLong = h2o_util.doubleToUnsignedLongLong(expectedFpSum)

                parseResult = h2i.import_parse(path=csvPathname, schema='put', hex_key=hex_key, 
                    timeoutSecs=3000, retryDelaySecs=2)
                inspect = h2o_cmd.runInspect(key=hex_key)
                print "numRows:", inspect['numRows']
                print "numCols:", inspect['numCols']
                inspect = h2o_cmd.runInspect(key=hex_key, offset=-1)
                print "inspect offset = -1:", h2o.dump_json(inspect)

                
                # looking at the 8 bytes of bits for the h2o doubles
                # xorsum will zero out the sign and exponent
                for execExpr in exprList:
                    for repeate in range(3):
                        start = time.time()
                        (execResult, fpResult) = h2e.exec_expr(h2o.nodes[0], execExpr, 
                            resultKey=None, timeoutSecs=300)
                        print 'exec took', time.time() - start, 'seconds'
                        print "execResult:", h2o.dump_json(execResult)
                        ullResult = h2o_util.doubleToUnsignedLongLong(fpResult)
                        ullResultList.append((ullResult, fpResult))

                        print "%30s" % "ullResult (0.16x):", "0x%0.16x   %s" % (ullResult, fpResult)
                        print "%30s" % "expectedUllSum (0.16x):", "0x%0.16x   %s" % (expectedUllSum, expectedUllSumAsDouble)
                        print "%30s" % "expectedFpSum (0.16x):", "0x%0.16x   %s" % (expectedFpSumAsLongLong, expectedFpSum)

                        # allow diff of the lsb..either way. needed when integers are parsed

                        # okay for a couple of lsbs to be wrong, due to conversion from stringk
                        # ullResult (0.16x): 0x02c1a21f923cee96   2.15698793923e-295
                        # expectedUllSum (0.16x): 0x02c1a21f923cee97   2.15698793923e-295
                        # expectedFpSum (0.16x): 0x42f054af32b3c408   2.87294442126e+14

                        # ullResult and expectedUllSum are Q ints, (64-bit) so can subtract them.
                        # I guess we don't even care about sign, since we zero the first 4 bits (xorsum) to avoid nan/inf issues
                        if ullResult!=expectedUllSum and (abs(ullResult-expectedUllSum)>ALLOWED_DELTA):
                            emsg = "h2o didn't get the same xorsum as python. 0x%0.16x 0x%0.16x" % (ullResult, expectedUllSum)
                            if STOP_ON_ERROR:
                                raise Exception(emsg)
                            else:  
                                print emsg

                        # print "%30s" % "hex(bitResult):", hex(ullResult)

                    h2o.check_sandbox_for_errors()

                    print "first result was from a sum. others are xorsum"
                    print "ullResultList:"
                    for ullResult, fpResult in ullResultList:
                        print "%30s" % "ullResult (0.16x):", "0x%0.16x   %s" % (ullResult, fpResult)

                    print "%30s" % "expectedUllSum (0.16x):", "0x%0.16x   %s" % (expectedUllSum, expectedUllSumAsDouble)
                    print "%30s" % "expectedFpSum (0.16x):", "0x%0.16x   %s" % (expectedFpSumAsLongLong, expectedFpSum)




if __name__ == '__main__':
    h2o.unit_main()
