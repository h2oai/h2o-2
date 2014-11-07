import unittest, time, sys, random, math, getpass
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_import as h2i, h2o_util, h2o_print as h2p

NA_POSS = ('', 'NA', '"NA"')
ZERO_POSS = ('0', ' 0', '0 ')
def write_syn_dataset(csvPathname, rowCount, colCount, SEED, choices):
    r1 = random.Random(SEED)
    dsf = open(csvPathname, "w+")

    naCnt = [0 for j in range(colCount)]
    for i in range(rowCount):
        rowData = []
        for j in range(colCount):
            ri = random.choice(choices)
            if (ri in ZERO_POSS) or (ri in NA_POSS):
                naCnt[j] += 1
            rowData.append(ri)
        rowDataCsv = ",".join(map(str,rowData))
        dsf.write(rowDataCsv + "\n")
    dsf.close()

    # FIX..temp hack to fix the naCnt if we only got 2 choices (assumes choices is always len(3) here
    assert len(choices)==3
    # I guess don't worry about case where 0 dominates, but there are other NA's  (besides the enums)
    # the numbers will dominate if single enum, not na. the enums na
    if choices[0]==choices[1] and choices[2] not in NA_POSS:
        for j in range(colCount):
            naCnt[j] = rowCount - naCnt[j]
    return naCnt

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_enums_with_0_NA(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()

        choicesList = [
            (' a', ' b',  'NA'),
            (' a', ' b',  '"NA"'),
            # only one enum?
            # the NA count has to get flipped if just one enum and 0
            (' a', ' b',  ''),
            (' a', ' a',  ''),
            # (' a', 'a',  '0'), # doesn't match my "single enum' check above
            (' a', ' b', ' 0'),
            # what about mixed NA and 0? doesn't happen?
            ('N', 'Y', '0'),
            ('n', 'y', '0'),
            ('F', 'T', '0'),
            ('f', 't', '0'),
            (' N', ' Y', ' 0'),
            (' n', ' y', ' 0'),
            (' F', ' T', ' 0'),
            (' f', ' t', ' 0'),
        ]

        # white space is stripped
        expectedList = [
            # only one enum?
            (' a', ' b',  ''),
            (' a', ' b',  ''),
            ('a', 'b', ''),
            ('a', 'a', ''),
            # ('a', 'a', '0'),
            ('a', 'b', '0'),
            ('N', 'Y', '0'),
            ('n', 'y', '0'),
            ('F', 'T', '0'),
            ('f', 't', '0'),
            ('N', 'Y', '0'),
            ('n', 'y', '0'),
            ('F', 'T', '0'),
            ('f', 't', '0'),
        ]

        tryList = [
            # colname, (min, 25th, 50th, 75th, max)
            (1000, 5, 'x.hex', choicesList[4], expectedList[4]),
            (1000, 5, 'x.hex', choicesList[5], expectedList[5]),
            (1000, 5, 'x.hex', choicesList[6], expectedList[6]),
            (1000, 5, 'x.hex', choicesList[7], expectedList[7]),
            (1000, 5, 'x.hex', choicesList[3], expectedList[3]),
            (1000, 5, 'x.hex', choicesList[2], expectedList[2]),
            (1000, 5, 'x.hex', choicesList[1], expectedList[1]),
            (1000, 5, 'x.hex', choicesList[0], expectedList[0]),
        ]

        timeoutSecs = 10
        trial = 1
        n = h2o.nodes[0]
        lenNodes = len(h2o.nodes)

        x = 0
        timeoutSecs = 60
        for (rowCount, colCount, hex_key, choices, expected) in tryList:
            # max error = half the bin size?
            print "choices:", choices
        
            SEEDPERFILE = random.randint(0, sys.maxint)
            x += 1

            csvFilename = 'syn_' + "binary" + "_" + str(rowCount) + 'x' + str(colCount) + '.csv'
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename
            csvPathnameFull = h2i.find_folder_and_filename(None, csvPathname, returnFullPath=True)

            print "Creating random", csvPathname
            expectedNaCnt = write_syn_dataset(csvPathname, rowCount, colCount, SEEDPERFILE, choices)
            # force header=0 so the T/F strings don't get deduced to be headers
            parseResult = h2i.import_parse(path=csvPathname, schema='put', hex_key=hex_key, header=0, timeoutSecs=10, doSummary=False)
            print "Parse result['destination_key']:", parseResult['destination_key']

            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
            print "\n" + csvFilename

            numRows = inspect["numRows"]
            numCols = inspect["numCols"]

            summaryResult = h2o_cmd.runSummary(key=hex_key, noPrint=False, numRows=numRows, numCols=numCols)
            h2o.verboseprint("summaryResult:", h2o.dump_json(summaryResult))

            # only one column

            for i in range(colCount):
                column = summaryResult['summaries'][i]

                colname = column['colname']
                coltype = column['type']
                nacnt = column['nacnt']
                # if it's just 0's with a single enum, the enums become NA, so the count is flipped
                self.assertEqual(nacnt, expectedNaCnt[i], "Column %s Expected %s. nacnt %s incorrect. choices: %s" % (i, expectedNaCnt[i], nacnt, choices))

                stats = column['stats']
                stattype= stats['type']
                self.assertEqual(stattype, 'Enum')

                # FIX! we should compare mean and sd to expected?
                cardinality = stats['cardinality']

                hstart = column['hstart']
                hstep = column['hstep']
                hbrk = column['hbrk']
                # cover the hacky two equal expected values
                hcnt = column['hcnt']
                if expected[0]==expected[1]:
                    self.assertEqual(hbrk, [expected[0]])
                    hcntTotal = hcnt[0]
                else: 
                    self.assertEqual(hbrk, [expected[0], expected[1]])
                    hcntTotal = hcnt[0] + hcnt[1]


                self.assertEqual(hcntTotal, rowCount - expectedNaCnt[i])

                self.assertEqual(rowCount, numRows, 
                    msg="numRows %s should be %s" % (numRows, rowCount))

            trial += 1


if __name__ == '__main__':
    h2o.unit_main()

