import unittest, random, sys, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i, h2o_exec as h2e, h2o_glm
import h2o_util
from collections import OrderedDict

zeroList = [
        'Result0 = 0',
]
# the first column should use this
exprList = [
        'Result<n> = sum(<keyX>[,<col1>])',
    ]

DO_SUMMARY = False
DO_COMPARE_SUM = False 
DO_BAD_SEED = False

def write_syn_dataset(csvPathname, rowCount, colCount, SEEDPERFILE, sel, distribution):
    # we can do all sorts of methods off the r object
    r = random.Random(SEEDPERFILE)

    def addRandValToRowStuff(colNumber, valMin, valMax, rowData, synColSumDict):
        # colNumber should not be 0, because the output will be there
        
        ## val = r.uniform(MIN,MAX)
        val = r.triangular(valMin,valMax,0)
        valFormatted = h2o_util.fp_format(val, sel)

        # force it to be zero in this range. so we don't print zeroes for svm!
        if (val > valMin/2) and (val < valMax/2):
            return None
        else:
            rowData.append(str(colNumber) + ":" + valFormatted) # f should always return string
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
    # ordinary dict
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
        global SEED
        if DO_BAD_SEED:
            SEED = h2o.setup_random_seed(seed=5605820711843900818)
        else:
            SEED = h2o.setup_random_seed()
        h2o.init(2,java_heap_GB=5)

    @classmethod
    def tearDownClass(cls):
        # h2o.sleep(3600)
        h2o.tear_down_cloud()

    def test_many_fp_formats_libsvm_2_fvec(self):
        #h2b.browseTheCloud()
        SYNDATASETS_DIR = h2o.make_syn_dir()
        tryList = [
            (100, 10000, 'cA', 300, 'sparse50'),
            (100, 10000, 'cB', 300, 'sparse'),
            # (100, 40000, 'cC', 300, 'sparse50'),
            # (100, 40000, 'cD', 300, 'sparse'),
            ]

        # h2b.browseTheCloud()
        for (rowCount, colCount, hex_key, timeoutSecs, distribution) in tryList:
            NUM_CASES = h2o_util.fp_format()
            for sel in [random.randint(0,NUM_CASES-1)]: # len(caseList)
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
                print "Parse result['destination_key']:", parseResult['destination_key']
                inspect = h2o_cmd.runInspect(None, parseResult['destination_key'], max_column_display=colNumberMax+1, timeoutSecs=timeoutSecs)
                numCols = inspect['numCols']
                numRows = inspect['numRows']
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

                self.assertEqual(colNumberMax+1, numCols, msg="generated %s cols (including output).  parsed to %s cols" % (colNumberMax+1, numCols))

                # Exec (column sums)*************************************************
                if DO_COMPARE_SUM:
                    h2e.exec_zero_list(zeroList)
                    colResultList = h2e.exec_expr_list_across_cols(None, exprList, selKey2, maxCol=colNumberMax+1,
                        timeoutSecs=timeoutSecs, print_params=False)
                    #print "\n*************"
                    #print "colResultList", colResultList
                    #print "*************"

                self.assertEqual(rowCount, numRows, msg="generated %s rows, parsed to %s rows" % (rowCount, numRows))
                # need to fix this for compare to expected
                # we should be able to keep the list of fp sums per col above
                # when we generate the dataset

                sortedColSumDict = OrderedDict(sorted(synColSumDict.items()))
                print sortedColSumDict
                for k,v in sortedColSumDict.iteritems():
                    print k
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
                    mean = float(inspect['cols'][k]['mean'])
                    # our fp formats in the syn generation sometimes only have two places?
                    if not h2o_util.approxEqual(mean, synMean, tol=1e-3):
                        execExpr = 'sum(%s[,%s])' % (selKey2, k+1)
                        resultExec = h2o_cmd.runExec(str=execExpr, timeoutSecs=300) 
                        print "Result of exec sum on failing col:..:", k, h2o.dump_json(resultExec)
                        print "Result of remembered sum on failing col:..:", k, v
                        print "Result of inspect mean * rowCount on failing col..:", mean * rowCount
                        print "k: ",k , "mean: ", mean, "remembered sum/rowCount : ", synMean
                        sys.stdout.flush()
                        raise Exception('col %s mean %0.6f is not equal to generated mean %0.6f' % (k, mean, synMean))

                    naCnt = inspect['cols'][k]['naCnt']
                    self.assertEqual(0, naCnt,
                        msg='col %s naCnt %d should be 0' % (k, naCnt))



if __name__ == '__main__':
    h2o.unit_main()
