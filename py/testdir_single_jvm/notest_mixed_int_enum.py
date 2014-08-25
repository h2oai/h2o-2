import unittest, time, sys, random, math, getpass
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_import as h2i, h2o_util, h2o_print as h2p

# fails with 1M and NA

print "Same as test_summary2_uniform.py except for every data row,"
print "5 rows of synthetic NA rows are added. results should be the same for quantiles"

def write_syn_dataset(csvPathname, rowCount, colCount, SEED, choices):
    r1 = random.Random(SEED)
    dsf = open(csvPathname, "w+")

    naCnt = [0 for j in range(colCount)]


    for i in range(rowCount):
        rowData = []
        for j in range(colCount):
            ri = random.choice(choices[0:1])
            rowData.append(ri)
        rowDataCsv = ",".join(map(str,rowData))
        dsf.write(rowDataCsv + "\n")

    # add one integer at the end. we should get one na?
    rowData = []
    for j in range(colCount):
        ri = 3
        rowData.append(ri)
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

    def test_NOPASS_mixed_int_enum(self):
        h2o.beta_features = True
        SYNDATASETS_DIR = h2o.make_syn_dir()

        choicesList = [
            ('abc', 'def', '0'),
        ]

        # white space is stripped
        expectedList = [
            ('abc', 'def', ''),
        ]

        tryList = [
            # colname, (min, 25th, 50th, 75th, max)
            (100, 200, 'x.hex', choicesList[0], expectedList[0]),
        ]

        timeoutSecs = 10
        trial = 1
        n = h2o.nodes[0]
        lenNodes = len(h2o.nodes)

        x = 0
        timeoutSecs = 60
        for (rowCount, colCount, hex_key, choices, expected) in tryList:
            # max error = half the bin size?
        
            SEEDPERFILE = random.randint(0, sys.maxint)
            x += 1

            csvFilename = 'syn_' + "binary" + "_" + str(rowCount) + 'x' + str(colCount) + '.csv'
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename
            csvPathnameFull = h2i.find_folder_and_filename(None, csvPathname, returnFullPath=True)

            print "Creating random", csvPathname
            expectedNaCnt = write_syn_dataset(csvPathname, rowCount, colCount, SEEDPERFILE, choices)
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
                nacnt = column['nacnt']
                self.assertEqual(nacnt, expectedNaCnt[i], "Column %s Expected %s. nacnt %s incorrect" % (i, expectedNaCnt[i], nacnt))

                stats = column['stats']
                stattype= stats['type']
                self.assertEqual(stattype, 'Enum')

                # FIX! we should compare mean and sd to expected?
                cardinality = stats['cardinality']

                hstart = column['hstart']
                hstep = column['hstep']
                hbrk = column['hbrk']
                self.assertEqual(hbrk, [expected[0], expected[1]])

                hcnt = column['hcnt']

                hcntTotal = hcnt[0] + hcnt[1]
                self.assertEqual(hcntTotal, rowCount - expectedNaCnt[i])

                self.assertEqual(rowCount, numRows, 
                    msg="numRows %s should be %s" % (numRows, rowCount))


            trial += 1

            h2i.delete_keys_at_all_nodes()


if __name__ == '__main__':
    h2o.unit_main()

