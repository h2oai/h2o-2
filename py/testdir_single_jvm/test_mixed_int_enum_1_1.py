import unittest, time, sys, random, math, getpass
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_import as h2i, h2o_util, h2o_print as h2p

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
        global SEED, localhost
        SEED = h2o.setup_random_seed()
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud()
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_mixed_int_enum_1_1(self):
        h2o.beta_features = True
        SYNDATASETS_DIR = h2o.make_syn_dir()

        # this should be a sorted list for comparing to hbrk in the histogram in h2o summary?
        enumList = ['abc', 'def', 'ghi']
        # numbers 1 and 2 may not be counted as NAs correctly? what about blank space?
        intList = [0, 1, 2, '']
        expectedList = [ 'abc', 'def', 'ghi']

        tryList = [
            # not sure about this case
            (100, 20, 'x.hex', enumList[0:1], expectedList[0:1], intList[0:2]),
            # colname, (min, 25th, 50th, 75th, max)
            (100, 20, 'x.hex', enumList[0:2], expectedList[0:2], intList[0:1]),
            # fails this case
            # (100, 200, 'x.hex', enumList[0:1], expectedList[0:1], intList[0:1]),
            (100, 20, 'x.hex', enumList[0: ], expectedList[0: ], intList[0:1]),
            (100, 20, 'x.hex', enumList[0:2], expectedList[0:2], intList[0:2]),
            # this case seems to fail
            (100, 20, 'x.hex', enumList[0:1], expectedList[0:1], intList[0:2]),
            # this seems wrong also
            (100, 20, 'x.hex', enumList[0: ], expectedList[0: ], intList[0:2]),
        ]

        timeoutSecs = 10
        trial = 1
        n = h2o.nodes[0]
        lenNodes = len(h2o.nodes)

        x = 0
        timeoutSecs = 60
        for (rowCount, colCount, hex_key, enumChoices, enumExpected, intChoices) in tryList:
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

                stats = column['stats']
                stattype= stats['type']
                self.assertEqual(stattype, 'Enum')

                # FIX! we should compare mean and sd to expected?
                cardinality = stats['cardinality']
                # assume enough rows to hit all of the small # of choices
                self.assertEqual(cardinality, len(enumChoices),
                    msg="trial %s: cardinality %s should be %s" % (trial, cardinality, len(enumChoices))) 

                hstart = column['hstart']
                hstep = column['hstep']
                hbrk = column['hbrk']
                # assume I create the list above in the same order that h2o will show the order. sorted?
                self.assertEqual(hbrk, enumChoices) 

                hcnt = column['hcnt']

                hcntTotal = sum(hcnt)
                numRowsCreated = rowCount + len(intChoices)
                self.assertEqual(hcntTotal, numRowsCreated - expectedNaCnt[i])

                self.assertEqual(numRows, numRowsCreated,
                    msg="trial %s: numRows %s should be %s" % (trial, numRows, numRowsCreated))

                nacnt = column['nacnt']
                self.assertEqual(nacnt, expectedNaCnt[i], 
                    "trial %s: Column %s Expected %s. nacnt %s incorrect" % (trial, i, expectedNaCnt[i], nacnt))
            trial += 1

            h2i.delete_keys_at_all_nodes()


if __name__ == '__main__':
    h2o.unit_main()

