import unittest, random, sys, time
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import as h2i, h2o_exec as h2e, h2o_glm

zeroList = [
        'Result0 = 0',
    ]

exprList = [
        'Result<n> = sum(<keyX>[,<col1>])',
    ]

DO_SUMMARY = True
valMin = -1e2
valMax =  1e2
classMin = 0
classMax = 255

def write_syn_dataset(csvPathname, rowCount, colCount, SEEDPERFILE, sel, distribution):
    # we can do all sorts of methods off the r object
    r = random.Random(SEEDPERFILE)

    def e0(val): return "%e" % val
    def e1(val): return "%20e" % val
    def e2(val): return "%-20e" % val
    def e3(val): return "%020e" % val
    def e4(val): return "%+e" % val
    def e5(val): return "%+20e" % val
    def e6(val): return "%+-20e" % val
    def e7(val): return "%+020e" % val
    def e8(val): return "%.4e" % val
    def e9(val): return "%20.4e" % val
    def e10(val): return "%-20.4e" % val
    def e11(val): return "%020.4e" % val
    def e12(val): return "%+.4e" % val
    def e13(val): return "%+20.4e" % val
    def e14(val): return "%+-20.4e" % val
    def e15(val): return "%+020.4e" % val

    def f0(val): return "%f" % val
    def f1(val): return "%20f" % val
    def f2(val): return "%-20f" % val
    def f3(val): return "%020f" % val
    def f4(val): return "%+f" % val
    def f5(val): return "%+20f" % val
    def f6(val): return "%+-20f" % val
    def f7(val): return "%+020f" % val
    def f8(val): return "%.4f" % val
    def f9(val): return "%20.4f" % val
    def f10(val): return "%-20.4f" % val
    def f11(val): return "%020.4f" % val
    def f12(val): return "%+.4f" % val
    def f13(val): return "%+20.4f" % val
    def f14(val): return "%+-20.4f" % val
    def f15(val): return "%+020.4f" % val

    def g0(val): return "%g" % val
    def g1(val): return "%20g" % val
    def g2(val): return "%-20g" % val
    def g3(val): return "%020g" % val
    def g4(val): return "%+g" % val
    def g5(val): return "%+20g" % val
    def g6(val): return "%+-20g" % val
    def g7(val): return "%+020g" % val
    def g8(val): return "%.4g" % val
    def g9(val): return "%20.4g" % val
    def g10(val): return "%-20.4g" % val
    def g11(val): return "%020.4g" % val
    def g12(val): return "%+.4g" % val
    def g13(val): return "%+20.4g" % val
    def g14(val): return "%+-20.4g" % val
    def g15(val): return "%+020.4g" % val

    # try a neat way to use a dictionary to case select functions
    # didn't want to use python advanced string format with variable as format
    # because they do left/right align outside of that??
    caseList=[
        e0, e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15,
        f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15,
        g0, g1, g2, g3, g4, g5, g6, g7, g8, g9, g10, g11, g12, g13, g14, g15,
        ]

    if sel<0 or sel>=len(caseList):
        raise Exception("sel out of range in write_syn_dataset:", sel)
    f = caseList[sel]

    def addRandValToRowStuff(colNumber, rowData, synColSumDict):
        # colNumber should not be 0, because the output will be there
        ## val = r.uniform(MIN,MAX)
        val = r.triangular(valMin,valMax,0)
        # force it to be zero in this range. so we don't print zeroes for svm!
        if colNumber==1 or (val > valMin/2) and (val < valMax/2):
            val = 0
        a = addValToRowStuff(colNumber, val, rowData, synColSumDict)
        return a

    def addValToRowStuff(colNumber, val, rowData, synColSumDict):
        # want to add here, so we can have cols with 0 expected value
        # but we need to track max col that actually goes in the libsvm, so we know
        # how many cols should be in the parsed data
        if colNumber in synColSumDict:
            synColSumDict[colNumber] += val # sum of column (dict)
        else:
            synColSumDict[colNumber] = val # sum of column (dict)

        # don't want to print zero values in row data, because if fp format, then h2o will parse to 4 bytes (even if 0)
        if val == 0:
            return None
        else:
            rowData.append(str(colNumber) + ":" + f(val)) # f should always return string
            return val

    #********************************************
    dsf = open(csvPathname, "w+")
    synColSumDict = {0: 0} # guaranteed to have col 0 for output
    # even though we try to get a max colCount with random, we might fall short
    # track what max we really got
    colNumberMax = 0
    for i in range(rowCount):
        rowData = []
        d = random.randint(0,2)
        # we need at least one with a value, otherwise h2o will parse as normal csv and not get "Target" etc col names
        # it will look like single col! hopefully rand is low enough to get at least one
        if d!=0:
            if distribution == 'sparse':
                # only one value per row!
                # is it okay to specify col 0 in svm? where does the output data go? (col 0)
                colNumber = random.randint(1, colCount)
                val = addRandValToRowStuff(colNumber, rowData, synColSumDict)
                if val:
                    colNumberMax = max(colNumberMax,colNumber)
            elif distribution == 'sparse50':
                # then some number of values per row constraine by max # of cols .. 50% or so?
                for colNumber in range(1, colCount+1):
                    val = addRandValToRowStuff(colNumber, rowData, synColSumDict)
                    if val:
                        colNumberMax = max(colNumberMax,colNumber)
            else:
                raise(Exception, "Don't understand what you want here %s" % distribution)

        # space is the only valid separator
        # add the output (col 0)
        # random integer for class
        val = random.randint(classMin,classMax)
        rowData.insert(0, val)
        synColSumDict[0] += val # sum of column (dict)

        rowDataCsv = " ".join(map(str,rowData))
        # FIX! vary the eol ?
        # randomly skip some rows. only write 1/3
        dsf.write(rowDataCsv + "\n")

    dsf.close()
    return (synColSumDict, colNumberMax)

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED, localhost
        SEED = h2o.setup_random_seed()
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(2,java_heap_GB=5)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_many_fp_formats_libsvm (self):
        h2b.browseTheCloud()
        SYNDATASETS_DIR = h2o.make_syn_dir()
        tryList = [
            (10, 10, 'cA', 30, 'sparse50'),
            (100, 10, 'cB', 30, 'sparse'),
            (100000, 100, 'cC', 30, 'sparse'),
            (1000, 10, 'cD', 30, 'sparse50'),
            (100, 100, 'cE', 30,'sparse'),
            (100, 100, 'cF', 30,'sparse50'),
            ]

        # h2b.browseTheCloud()
        for (rowCount, colCount, hex_key, timeoutSecs, distribution) in tryList:
            # for sel in range(48): # len(caseList)
            for sel in [random.randint(0,47)]: # len(caseList)
                SEEDPERFILE = random.randint(0, sys.maxint)
                csvFilename = "syn_%s_%s_%s_%s.csv" % (SEEDPERFILE, sel, rowCount, colCount)
                csvPathname = SYNDATASETS_DIR + '/' + csvFilename

                print "Creating random", csvPathname
                # dict of col sums for comparison to exec col sums below
                (synColSumDict, colNumberMax)  = write_syn_dataset(csvPathname, rowCount, colCount, SEEDPERFILE, sel, distribution)

                selKey2 = hex_key + "_" + str(sel)
                parseResult = h2i.import_parse(path=csvPathname, schema='put', hex_key=selKey2, timeoutSecs=timeoutSecs)
                print csvFilename, 'parse time:', parseResult['response']['time']
                print "Parse result['destination_key']:", parseResult['destination_key']
                inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
                num_cols = inspect['num_cols']
                num_rows = inspect['num_rows']
                print "\n" + csvFilename

                # SUMMARY****************************************
                # gives us some reporting on missing values, constant values, 
                # to see if we have x specified well
                # figures out everything from parseResult['destination_key']
                # needs y to avoid output column (which can be index or name)
                # assume all the configs have the same y..just check with the firs tone
                goodX = h2o_glm.goodXFromColumnInfo(y=0,
                    key=parseResult['destination_key'], timeoutSecs=300)

                if DO_SUMMARY:
                    summaryResult = h2o_cmd.runSummary(key=selKey2, timeoutSecs=360)
                    h2o_cmd.infoFromSummary(summaryResult, noPrint=True)


                # we might have added some zeros at the end, that our colNumberMax won't include
                print synColSumDict.keys(), colNumberMax
                self.assertEqual(colNumberMax+1, num_cols, 
                    msg="generated %s cols (including output).  parsed to %s cols" % (colNumberMax+1, num_cols))

                # Exec (column sums)*************************************************
                h2e.exec_zero_list(zeroList)
                # how do we know the max dimension (synthetic may not generate anything for the last col)
                # use num_cols?. num_cols should be <= colCount. 

                colSumList = h2e.exec_expr_list_across_cols(None, exprList, selKey2, maxCol=colNumberMax+1,
                    timeoutSecs=timeoutSecs)

                self.assertEqual(rowCount, num_rows, msg="generated %s rows, parsed to %s rows" % (rowCount, num_rows))
                # need to fix this for compare to expected
                # we should be able to keep the list of fp sums per col above
                # when we generate the dataset
                print "\ncolSumList:", colSumList
                print "\nsynColSumDict:", synColSumDict

                for k,v in synColSumDict.iteritems():
                    if k > colNumberMax: # ignore any extra 0 cols at the end
                        continue

                    # k should be integers that match the number of cols
                    self.assertTrue(k>=0 and k<len(colSumList), msg="k: %s len(colSumList): %s num_cols: %s" % (k, len(colSumList), num_cols))

                    syn = {}
                    if k==0: 
                        syn['name'] = "Target"
                        syn['size'] = {1,2} # can be two if we actually used the full range 0-255 (need extra for h2o NA)
                        syn['type'] = {'int'}
                        syn['min'] = classMin
                        syn['max'] = classMax
                        # don't check these for the col 0 'Target'
                        syn['scale'] = {1}
                        # syn['base'] = 0
                        # syn['variance'] = 0
                    elif k==1: # we forced this to always be 0
                        syn['name'] = "V" + str(k)
                        syn['size'] = {1}
                        syn['type'] = {'int'}
                        syn['min'] = 0
                        syn['max'] = 0
                        syn['scale'] = {1}
                        syn['base'] = 0
                        syn['variance'] = 0
                    else:
                        syn['name'] = "V" + str(k)
                        syn['size'] = {1,2,4,8} # can be 2, 4 or 8? maybe make this a set for membership check
                        syn['type'] = {'int', 'float'}
                        syn['min'] = valMin
                        syn['max'] = valMax
                        syn['scale'] = {1,10,100,1000}
                        # syn['base'] = 0
                        # syn['variance'] = 0

                    syn['num_missing_values'] = 0
                    syn['enum_domain_size'] = 0
                    # syn['min'] = 0
                    # syn['max'] = 0
                    # syn['mean'] = 0

                    cols = inspect['cols'][k]
                    for synKey in syn:
                        # we may not see the min/max range of values that was bounded by our gen, but 
                        # we can check that it's a subset of the allowed range
                        if synKey == 'min':
                            self.assertTrue(syn[synKey] <= cols[synKey],
                                msg='col %s %s %s should be <= %s' % (k, synKey, cols[synKey], syn[synKey]))
                        elif synKey == 'max':
                            self.assertTrue(syn[synKey] >= cols[synKey],
                                msg='col %s %s %s should be >= %s' % (k, synKey, cols[synKey], syn[synKey]))
                        elif synKey == 'size' or synKey == 'scale' or synKey == 'type':
                            if cols[synKey] not in syn[synKey]:
                                # for debug of why it was a bad size
                                print "cols size/min/max:", cols['size'], cols['min'], cols['max']
                                print "syn size/min/max:", syn['size'], syn['min'], syn['max']
                                raise Exception('col %s %s %s should be in this allowed %s' % (k, synKey, cols[synKey], syn[synKey]))
                        else:
                            self.assertEqual(syn[synKey], cols[synKey],
                                msg='col %s %s %s should be %s' % (k, synKey, cols[synKey], syn[synKey]))
                    
                    colSum = colSumList[k]
                    print "\nComparing col", k, "sums:", v, colSum
                    # Even though we're comparing floating point sums, the operations probably should have
                    # been done in same order, so maybe the comparison can be exact (or not!)
                    self.assertAlmostEqual(float(v), colSum, places=0, 
                        msg='%0.6f col sum is not equal to expected %0.6f' % (v, colSum))


if __name__ == '__main__':
    h2o.unit_main()
