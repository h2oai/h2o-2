import unittest, random, sys, time, os
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import as h2i
import h2o_exec as h2e
import codecs, string

# ord('a') gives 97
# str(unichr(97)) gives 'a' 

# MAX_CHAR = 255
MAX_CHAR = 127
JUST_EASY_CHARS = True
JUST_EASIER_CHARS = True

print "Info: If the random chars are all numbers, then quoting them doesn't protect from being seen as number"
print "Need to add inspect/summary checking to the parsed result"
def write_syn_dataset(csvPathname, rowCount, colCount, SEED):
    r1 = random.Random(SEED)
    # dsf = open(csvPathname, "w+")
    # want the full unicode!, not just 0-127 ascii
    dsf = codecs.open(csvPathname,'w+','utf-8')

    for i in range(rowCount):
        rowData = []
        if JUST_EASIER_CHARS:
            legalChars = string.letters + string.punctuation + string.digits
        else:
            # same as this
            # legalChars = string.letters + string.punctuation + string.digits + string.whitespace
            legalChars = string.printable

        for j in range(colCount):
            # should be able to handle any character in a quoted stringexcept our 3 eol chars
            # create a random length enum. start with 1024 everywhere
            longEnum = ""
            for k in range(3):
                if JUST_EASY_CHARS | JUST_EASIER_CHARS:
                    uStr = random.choice(legalChars)
                else:
                    u = random.randint(0,MAX_CHAR)
                    uStr = str(unichr(u))
                # we're using single quote below, so don't use that either! double quote is ok!
                # comma is okay? we're going to force the separator below
                if uStr=="\n" or uStr=="\r\n" or uStr=="\r" or uStr=="'":
                    # translate to 'a'
                    uStr = 'a'
                longEnum += uStr
            rowData.append("'" + longEnum + "'") # single quoted long enum

        # can't use ', ' ..illegal, has to be comma only
        rowDataCsv = ",".join(map(str,rowData))
        dsf.write(rowDataCsv + "\n")

    dsf.close()


class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED, localhost
        SEED = h2o.setup_random_seed()
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(3, java_heap_GB=1) # enum processing is more interesting with multiple jvms
        else:
            h2o_hosts.build_cloud_with_hosts()


    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()


    def test_many_cols_with_syn(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        tryList = [
            (10, 100, 'cA', 5),
            (10, 100, 'cA', 5),
            (10, 100, 'cA', 5),
            (10, 100, 'cA', 5),
            (10, 100, 'cA', 5),
            (10, 100, 'cA', 5),
            (10, 100, 'cA', 5),
            (10, 100, 'cA', 5),
            ]

        ### h2b.browseTheCloud()
        lenNodes = len(h2o.nodes)

        cnum = 0
        for (rowCount, colCount, key2, timeoutSecs) in tryList:
            cnum += 1
            csvFilename = 'syn_' + str(SEED) + "_" + str(rowCount) + 'x' + str(colCount) + '.csv'
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename

            print "Creating random", csvPathname
            write_syn_dataset(csvPathname, rowCount, colCount, SEED)

            SEPARATOR = ord(',')
            parseKey = h2o_cmd.parseFile(None, csvPathname, key2=key2, timeoutSecs=10, separator=SEPARATOR)
            print csvFilename, 'parse time:', parseKey['response']['time']
            print "Parse result['destination_key']:", parseKey['destination_key']

            # We should be able to see the parse result?
            inspect = h2o_cmd.runInspect(None, parseKey['destination_key'])
            print "\n" + csvFilename

            if not h2o.browse_disable:
                h2b.browseJsonHistoryAsUrlLastMatch("Inspect")
                time.sleep(5)

            # try new offset/view
            inspect = h2o_cmd.runInspect(None, parseKey['destination_key'])


if __name__ == '__main__':
    h2o.unit_main()
