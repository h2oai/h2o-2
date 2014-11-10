import unittest
import random, sys, time, os
sys.path.extend(['.','..','../..','py'])

import h2o, h2o_cmd, h2o_import as h2i, h2o_exec as h2e
import codecs

print "This test makes sure python creates a utf16 file, that is not a ascii file"
print "apparently need to have at least one normal character otherwise the parse doesn't work right"

UTF16 = True
UTF8 = False
UTF8_MULTIBYTE = False

if UTF8:
    # what about multi-byte UTF8
    ordinalChoices = range(0x0, 0x100) # doesn't include last value ..allow ff
else:  # ascii subset?
    ordinalChoices = range(0x0, 0x80) # doesn't include last value ..allow 7f


if UTF8_MULTIBYTE:
    # add some UTF8 multibyte, and restrict the choices to make sure we hit these
    ordinalChoices = range(0x0, 0x40) # doesn't include last value ..allow 7f
    ordinalChoices += [0x201c, 0x201d, 0x2018, 0x2019, 6000]

ordinalChoices.remove(0x09) # is 9 bad..apparently can cause NA

ordinalChoices.remove(0x00) # nul
ordinalChoices.remove(0x0d) # cr
ordinalChoices.remove(0x0a) # lf
ordinalChoices.remove(0x01) # hiveseparator

# smaller range, avoiding 0-1f control chars
# ordinalChoices = range(0x20, 0x7f) # doesn't include last value
ordinalChoices.remove(0x3b) # semicolon
ordinalChoices.remove(0x20) # space
ordinalChoices.remove(0x22) # double quote
# ordinalChoices.remove(0x27) # apostrophe. should be legal if single quotes not enabled
ordinalChoices.remove(0x2c) # comma

ordinalChoices.remove(0x30) # 0
ordinalChoices.remove(0x31) # 1
ordinalChoices.remove(0x32) # 2
ordinalChoices.remove(0x33) # 3
ordinalChoices.remove(0x34) # 4
ordinalChoices.remove(0x35) # 5
ordinalChoices.remove(0x36) # 6
ordinalChoices.remove(0x37) # 7
ordinalChoices.remove(0x38) # 8
ordinalChoices.remove(0x39) # 9
# print ordinalChoices

def generate_random_utf8_string(length=1):
        cList = []
        for i in range(length):
            r = random.choice(ordinalChoices)
            c = unichr(r).encode('utf-8')
            cList.append(c)
            print 

        # this is a random byte string now, of type string?
        return "".join(cList)

def write_syn_dataset(csvPathname, rowCount, colCount, SEED):
    r1 = random.Random(SEED)
    if UTF8 or UTF8_MULTIBYTE:
        dsf = codecs.open(csvPathname, encoding='utf-8', mode='w+')
    elif UTF16:
        dsf = codecs.open(csvPathname, encoding='utf-16', mode='w+')
    else:
        dsf = open(csvPathname, "w+")

    for i in range(rowCount):
        if UTF16:
            u = unichr(0x2018) + unichr(6000) + unichr(0x2019)
            rowDataCsv = u
        else: # both ascii and utf-8 go here?
            rowData = []
            for j in range(colCount):
                r = generate_random_utf8_string(length=2)
                rowData.append(r)
            rowDataCsv = ",".join(rowData)
        if UTF16:
            # we're already passing it unicode. no decoding needed
            print "utf16:", repr(rowDataCsv), type(rowDataCsv)
            decoded = rowDataCsv
        else:
            print "str:", repr(rowDataCsv), type(rowDataCsv)
            decoded = rowDataCsv.decode('utf-8')
            # this has the right length..multibyte utf8 are decoded 
            print "utf8:" , repr(decoded), type(decoded)
        
        # dsf.write(rowDataCsv + "\n")
        dsf.write(decoded + "\n")
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

    def test_parse_rand_utf16(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        tryList = [
            (1000, 1, 'cA', 120),
            (1000, 1, 'cG', 120),
            (1000, 1, 'cH', 120),
            ]

        print "What about messages to log (INFO) about unmatched quotes (before eol)"
        # got this ..trying to avoid for now
        # Exception: rjson error in parse: Argument 'source_key' error: Parser setup appears to be broken, got AUTO

        for (rowCount, colCount, hex_key, timeoutSecs) in tryList:
            SEEDPERFILE = random.randint(0, sys.maxint)
            csvFilename = 'syn_' + str(SEEDPERFILE) + "_" + str(rowCount) + 'x' + str(colCount) + '.csv'
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename

            print "\nCreating random", csvPathname
            write_syn_dataset(csvPathname, rowCount, colCount, SEEDPERFILE)
            parseResult = h2i.import_parse(path=csvPathname, schema='put', header=0,
                hex_key=hex_key, timeoutSecs=timeoutSecs, doSummary=False)
            print "Parse result['destination_key']:", parseResult['destination_key']
            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'], timeoutSecs=60)
        
            print "inspect:", h2o.dump_json(inspect)
            numRows = inspect['numRows']
            self.assertEqual(numRows, rowCount, msg='Wrong numRows: %s %s' % (numRows, rowCount))
            numCols = inspect['numCols']
            self.assertEqual(numCols, colCount, msg='Wrong numCols: %s %s' % (numCols, colCount))

            for k in range(colCount):
                naCnt = inspect['cols'][k]['naCnt']
                self.assertEqual(0, naCnt, msg='col %s naCnt %d should be 0' % (k, naCnt))

                stype = inspect['cols'][k]['type']
                self.assertEqual("Enum", stype, msg='col %s type %s should be Enum' % (k, stype))

        #**************************
        # for background knowledge; (print info)
        import unicodedata
        # u = unichr(233) + unichr(0x0bf2) + unichr(3972) + unichr(6000) + unichr(13231)
        # left and right single quotes
        u = unichr(0x201c) + unichr(0x201d)
        # preferred apostrophe (right single quote)
        u = unichr(0x2019) 
        u = unichr(0x2018) + unichr(6000) + unichr(0x2019)
        # grave and acute?
        # u = unichr(0x60) + unichr(0xb4)
        # don't do this. grave with apostrophe http://www.cl.cam.ac.uk/~mgk25/ucs/quotes.html
        # u = unichr(0x60) + unichr(0x27)

        for i, c in enumerate(u):
            print i, '%04x' % ord(c), unicodedata.category(c),
            print unicodedata.name(c)

        # Get numeric value of second character
        # print unicodedata.numeric(u[1])
        #**************************

if __name__ == '__main__':
    h2o.unit_main()
