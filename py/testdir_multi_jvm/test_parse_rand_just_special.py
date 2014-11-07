import unittest, random, sys, time, os
sys.path.extend(['.','..','../..','py'])

import h2o, h2o_cmd, h2o_import as h2i, h2o_exec as h2e
import codecs

print "apparently need to have at least one normal character otherwise the parse doesn't work right"
print "just use the special ascii chars that h2o responds to"

# make choices just a string, since we extract one char at a time
# don't include CR/CRLF/LF
# just include a couple of numbers. 0 might be more special.
choices = "eE.120gGfFx@#,;\t "
# add single and double quote
choices += "'"
choices += '"'
# get the list of ordinals for those chars
ordinalChoices = [ord(choices[i]) for i in range(len(choices))]

def generate_random_utf8_string(length=1, multi=False, row=0, col=0):
    # want to handle more than 256 numbers
    cList = []
    firstRowCol = (row==0 and col==0)
    for i in range(length):
        r = random.choice(ordinalChoicesMulti if multi else ordinalChoices)
        # avoid # or @ comment in first char, first line
        # don't use " as first char, since that makes us drop rows
        while ((firstRowCol and (r==0x40 or r==0x23)) or r==0x22): # @ is 0x40, # is 0x23, " is 0x22
            # rechoose
            r = random.choice(ordinalChoicesMulti if multi else ordinalChoices)
        cList.append(unichr(r))
    return "".join(cList)

def write_syn_dataset(csvPathname, rowCount, colCount, colSepChar=",", rowSepChar="\n", SEED=12345678):
    r1 = random.Random(SEED)
    dsf = codecs.open(csvPathname, encoding='utf-8', mode='w+')
    for i in range(rowCount):
        rowData = []
        for j in range(colCount):
            r = generate_random_utf8_string(length=128, row=i, col=j)
            rowData.append(r)
        rowDataCsv = colSepChar.join(rowData)
        # use a random choice on the rowSepChar
        # 0xa is LF, 0xd is CR
        a = random.choice([chr(0xa), chr(0xd), chr(0xd) + chr(0xa)])
        dsf.write(rowDataCsv + a)
    dsf.close()

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init(2,java_heap_GB=1,use_flatfile=True)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_parse_rand_just_special(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        tryList = [
            # do two cols to detect bad eol behavior
            (1000, 2, 'cA', 120),
            (1000, 2, 'cG', 120),
            (1000, 2, 'cH', 120),
            ]

        print "What about messages to log (INFO) about unmatched quotes (before eol)"

        for repeat in range(1):
            for (rowCount, colCount, hex_key, timeoutSecs) in tryList:
                SEEDPERFILE = random.randint(0, sys.maxint)
                csvFilename = 'syn_' + str(SEEDPERFILE) + "_" + str(rowCount) + 'x' + str(colCount) + '.csv'
                csvPathname = SYNDATASETS_DIR + '/' + csvFilename

                print "\nCreating random", csvPathname
                write_syn_dataset(csvPathname, rowCount, colCount, SEED=SEEDPERFILE)
                parseResult = h2i.import_parse(path=csvPathname, schema='put', header=0,
                    hex_key=hex_key, timeoutSecs=timeoutSecs, doSummary=False)
                print "Parse result['destination_key']:", parseResult['destination_key']
                inspect = h2o_cmd.runInspect(None, parseResult['destination_key'], timeoutSecs=60)
            
                print "inspect:", h2o.dump_json(inspect)
                numRows = inspect['numRows']
                self.assertEqual(numRows, rowCount, msg='Wrong numRows: %s %s' % (numRows, rowCount))
                numCols = inspect['numCols']
                # no way to guess what the col count should be
                # self.assertEqual(numCols, colCount, msg='Wrong numCols: %s %s' % (numCols, colCount))

                # for k in range(colCount):
                    # no way to guess naCnt
                    # naCnt = inspect['cols'][k]['naCnt']
                    # self.assertEqual(0, naCnt, msg='col %s naCnt %d should be 0' % (k, naCnt))

                    # no way to guess type
                    # stype = inspect['cols'][k]['type']
                    # self.assertEqual("Enum", stype, msg='col %s type %s should be Enum' % (k, stype))

if __name__ == '__main__':
    h2o.unit_main()
