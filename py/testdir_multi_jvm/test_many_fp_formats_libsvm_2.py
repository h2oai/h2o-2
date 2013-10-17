import unittest, random, sys, time
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import as h2i, h2o_exec as h2e, h2o_glm

zeroList = [
        'Result0 = 0',
]
# the first column should use this
exprList = [
        'Result<n> = sum(<keyX>[<col1>])',
    ]

DO_SUMMARY = False
DO_COMPARE_SUM = False

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

    def addRandValToRowStuff(colNumber, valMin, valMax, rowData, synColSumDict):
        # colNumber should not be 0, because the output will be there
        
        ## val = r.uniform(MIN,MAX)
        val = r.triangular(valMin,valMax,0)
        # force it to be zero in this range. so we don't print zeroes for svm!
        if (val > valMin/2) and (val < valMax/2):
            return None
        else:
            rowData.append(str(colNumber) + ":" + f(val)) # f should always return string
            if colNumber in synColSumDict:
                synColSumDict[colNumber] += val # sum of column (dict)
            else:
                synColSumDict[colNumber] = val # sum of column (dict)
            return val

    valMin = -1e2
    valMax =  1e2
    classMin = -36
    classMax = 36
    dsf = open(csvPathname, "w+")
    synColSumDict = {0: 0} # guaranteed to have col 0 for output
    # even though we try to get a max colCount with random, we might fall short
    # track what max we really got
    colNumberMax = 0
    for i in range(rowCount):
        rowData = []
        d = random.randint(0,2)
        if d==0:
            if distribution == 'sparse':
                # only one value per row!
                # is it okay to specify col 0 in svm? where does the output data go? (col 0)
                colNumber = random.randint(1, colCount)
                val = addRandValToRowStuff(colNumber, valMin, valMax, rowData, synColSumDict)
                # did we add a val?
                if val and (colNumber > colNumberMax):
                    colNumberMax = colNumber
            else:
                # some number of values per row.. 50% or so?
                for colNumber in range(1, colCount+1):
                    val = addRandValToRowStuff(colNumber, valMin, valMax, rowData, synColSumDict)
                    if val and (colNumber > colNumberMax):
                        colNumberMax = colNumber

            # always need an output class, even if no cols are non-zero

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
    return (colNumberMax, synColSumDict)

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

    def test_many_fp_formats_libsvm_2 (self):
        h2b.browseTheCloud()
        SYNDATASETS_DIR = h2o.make_syn_dir()
        tryList = [
            (100, 10000, 'cA', 300, 'sparse50'),
            (100, 10000, 'cB', 300, 'sparse'),
            # (100, 40000, 'cC', 300, 'sparse50'),
            # (100, 40000, 'cD', 300, 'sparse'),
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
                (colNumberMax, synColSumDict) = write_syn_dataset(csvPathname, rowCount, colCount, SEEDPERFILE, sel, distribution)

                selKey2 = hex_key + "_" + str(sel)
                print "This dataset requires telling h2o parse it's a libsvm..doesn't detect automatically"
                parseResult = h2i.import_parse(path=csvPathname, schema='put', hex_key=selKey2, 
                    timeoutSecs=timeoutSecs, doSummary=False, parser_type='SVMLight')
                print csvFilename, 'parse time:', parseResult['response']['time']
                print "Parse result['destination_key']:", parseResult['destination_key']
                inspect = h2o_cmd.runInspect(None, parseResult['destination_key'], max_column_display=colNumberMax+1, timeoutSecs=timeoutSecs)
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
                    key=parseResult['destination_key'], timeoutSecs=300, noPrint=True)

                if DO_SUMMARY:
                    summaryResult = h2o_cmd.runSummary(key=selKey2, max_column_display=colNumberMax+1, timeoutSecs=timeoutSecs)
                    h2o_cmd.infoFromSummary(summaryResult, noPrint=True)

                self.assertEqual(colNumberMax+1, num_cols, msg="generated %s cols (including output).  parsed to %s cols" % (colNumberMax+1, num_cols))

                # Exec (column sums)*************************************************
                if DO_COMPARE_SUM:
                    h2e.exec_zero_list(zeroList)
                    colResultList = h2e.exec_expr_list_across_cols(None, exprList, selKey2, maxCol=colNumberMax+1,
                        timeoutSecs=timeoutSecs)
                    print "\n*************"
                    print "colResultList", colResultList
                    print "*************"

                self.assertEqual(rowCount, num_rows, msg="generated %s rows, parsed to %s rows" % (rowCount, num_rows))
                # need to fix this for compare to expected
                # we should be able to keep the list of fp sums per col above
                # when we generate the dataset
                ### print "\nsynColSumDict:", synColSumDict

                for k,v in synColSumDict.iteritems():
                    if DO_COMPARE_SUM:
                        # k should be integers that match the number of cols
                        self.assertTrue(k>=0 and k<len(colResultList))
                        compare = colResultList[k]
                        print "\nComparing col sums:", v, compare
                        # Even though we're comparing floating point sums, the operations probably should have
                        # been done in same order, so maybe the comparison can be exact (or not!)
                        self.assertAlmostEqual(v, compare, places=0, 
                            msg='%0.6f col sum is not equal to expected %0.6f' % (v, compare))

                    synMean = (v + 0.0)/rowCount
                    # enums don't have mean, but we're not enums
                    mean = inspect['cols'][k]['mean']
                    # our fp formats in the syn generation sometimes only have two places?
                    self.assertAlmostEqual(mean, synMean, places=0,
                        msg='col %s mean %0.6f is not equal to generated mean %0.6f' % (k, mean, synMean))

                    num_missing_values = inspect['cols'][k]['num_missing_values']
                    self.assertEqual(0, num_missing_values,
                        msg='col %s num_missing_values %d should be 0' % (k, num_missing_values))



if __name__ == '__main__':
    h2o.unit_main()
