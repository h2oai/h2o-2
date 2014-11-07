import unittest, time, sys, random, math, getpass
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_import as h2i, h2o_util, h2o_print as h2p, h2o_browse as h2b


ENABLE_ASSERTS = False
ROWS = 1000000
COLS = 2

def write_syn_dataset(csvPathname, rowCount, colCount, SEED, enumChoices, intChoices):
    r1 = random.Random(SEED)
    dsf = open(csvPathname, "w+")

    naCnt = [0 for j in range(colCount)]

    for i in range(rowCount):
        rowData = []
        for j in range(colCount):
            ri = random.choice(enumChoices)
            rowData.append(ri)
        rowDataCsv = ",".join(map(str,rowData))
        dsf.write(rowDataCsv + "\n")

    # add rows for each intChoice at the end. so you can have 0, 1 or ?? rows of ints
    # note it not's randomly distributed. deal with that later
    if intChoices:
        for choice in intChoices:
            rowData = []
            for j in range(colCount):
                rowData.append(choice)
                naCnt[j] += 1
            rowDataCsv = ",".join(map(str,rowData))
            dsf.write(rowDataCsv + "\n")

    dsf.close()
    return naCnt

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init(3)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_mixed_int_enum_many(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()

        # this should be a sorted list for comparing to hbrk in the histogram in h2o summary?
        enumList = ['abc', 'def', 'ghi']
        # numbers 1 and 2 may not be counted as NAs correctly? what about blank space?
        intList = [0, 1, 2, '']
        expectedList = [ 'abc', 'def', 'ghi']

        tryList = [
            # not sure about this case
            # some of the cases interpret as ints now (not as enum)
            (ROWS, COLS, 'a.hex', enumList[0:1], expectedList[0:1], intList[0:2], False),
            # colname, (min, COLS5th, 50th, 75th, max)
            (ROWS, COLS, 'b.hex', enumList[0:2], expectedList[0:2], intList[0:1], True),
            # fails this case
            (ROWS, COLS, 'c.hex', enumList[0:1], expectedList[0:1], intList[0:1], True),
            (ROWS, COLS, 'd.hex', enumList[0: ], expectedList[0: ], intList[0:1], True),
            (ROWS, COLS, 'e.hex', enumList[0:2], expectedList[0:2], intList[0:2], True),
            # this case seems to fail
            (ROWS, COLS, 'f.hex', enumList[0:1], expectedList[0:1], intList[0:2], True),
            # this seems wrong also
            (ROWS, COLS, 'g.hex', enumList[0: ], expectedList[0: ], intList[0:2], True),
        ]

        timeoutSecs = 10
        trial = 1
        n = h2o.nodes[0]
        lenNodes = len(h2o.nodes)

        x = 0
        timeoutSecs = 60
        for (rowCount, colCount, hex_key, enumChoices, enumExpected, intChoices, resultIsEnum) in tryList:
            # max error = half the bin size?
        
            SEEDPERFILE = random.randint(0, sys.maxint)
            x += 1

            csvFilename = 'syn_' + "binary" + "_" + str(rowCount) + 'x' + str(colCount) + '.csv'
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename
            csvPathnameFull = h2i.find_folder_and_filename(None, csvPathname, returnFullPath=True)

            print "Creating random", csvPathname
            expectedNaCnt = write_syn_dataset(csvPathname, rowCount, colCount, SEEDPERFILE, enumChoices, intChoices)
            parseResult = h2i.import_parse(path=csvPathname, schema='put', header=0,
                hex_key=hex_key, timeoutSecs=10, doSummary=False)
            print "Parse result['destination_key']:", parseResult['destination_key']

            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
            print "\nTrial:", trial, csvFilename

            numRows = inspect["numRows"]
            numCols = inspect["numCols"]

            summaryResult = h2o_cmd.runSummary(key=hex_key, noPrint=False, numRows=numRows, numCols=numCols)
            h2o.verboseprint("summaryResult:", h2o.dump_json(summaryResult))

            for i in range(colCount):
                column = summaryResult['summaries'][i]

                colname = column['colname']
                coltype = column['type']

                stats = column['stats']
                stattype= stats['type']
                if ENABLE_ASSERTS and resultIsEnum:
                    self.assertEqual(stattype, 'Enum', "trial %s: Expecting summaries/stats/type to be Enum for %s col colname %s" % (trial, i, colname))

                # FIX! we should compare mean and sd to expected?
                # assume enough rows to hit all of the small # of choices
                if ENABLE_ASSERTS and resultIsEnum:
                    # not always there
                    cardinality = stats['cardinality']
                    self.assertEqual(cardinality, len(enumChoices),
                        msg="trial %s: cardinality %s should be %s" % (trial, cardinality, len(enumChoices))) 

                hstart = column['hstart']
                hstep = column['hstep']
                hbrk = column['hbrk']
                # assume I create the list above in the same order that h2o will show the order. sorted?
                if ENABLE_ASSERTS and resultIsEnum:
                    self.assertEqual(hbrk, enumChoices) 

                hcnt = column['hcnt']

                hcntTotal = sum(hcnt)
                numRowsCreated = rowCount + len(intChoices)
                if ENABLE_ASSERTS and resultIsEnum:
                    self.assertEqual(hcntTotal, numRowsCreated - expectedNaCnt[i])

                self.assertEqual(numRows, numRowsCreated,
                    msg="trial %s: numRows %s should be %s" % (trial, numRows, numRowsCreated))

                nacnt = column['nacnt']
                if ENABLE_ASSERTS and resultIsEnum:
                    self.assertEqual(nacnt, expectedNaCnt[i], 
                        "trial %s: Column %s Expected %s. nacnt %s incorrect" % (trial, i, expectedNaCnt[i], nacnt))


                # FIX! no checks for the case where it got parsed as int column!
            trial += 1

            # h2i.delete_keys_at_all_nodes()


if __name__ == '__main__':
    h2o.unit_main()

