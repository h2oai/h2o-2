import unittest
import random, sys, time, os
sys.path.extend(['.','..','../..','py'])

# good background here. I don't really follow the recommendations in this test
# http://www.azavea.com/blogs/labs/2014/03/solving-unicode-problems-in-python-2-7/
import h2o, h2o_cmd, h2o_import as h2i, h2o_exec as h2e
import codecs

# This shows the test really created a UTF8 file that was not a ASCII file
# ~/h2o/py/testdir_multi_jvm$ file sandbox/syn*/*
# sandbox/syn_datasets/syn_3234802987159914820_1000x1.csv: UTF-8 Unicode text
# sandbox/syn_datasets/syn_7454586956682649267_1000x1.csv: UTF-8 Unicode text
#sandbox/syn_datasets/syn_8233902282973358813_1000x1.csv: UTF-8 Unicode text

print "This test makes sure python creates a utf8 file, that is not a ascii file"
print "apparently need to have at least one normal character otherwise the parse doesn't work right"
print "I think i have some multi-byte utf8 in there now"

# Interesting. Microsoft Word might introduce it's own super ascii/ (smart quotes)
# 145, 146, 147, 148, 151
# single quote (L/R), double quote (L/R), dash)

# https://0xdata.atlassian.net/browse/HEX-1950
# inconsistent handling of some utf-8 char encodings (NA vs not-NA)
# 0x08 is not treated as NA . It's enum 
# 0x09 is treated as NA 
# 0x00 thru 0x1f are considered control characters. 
# they match ASCII 0x00 thru 0x1F . I guess 0x7F is considered a control character too (DEL), (not printable) 

# del (127) is a control character, so don't include it
# I suppose we have to exclude space (0x20) to avoid NA
# and 0x22 is double quote, and 0x2c is comma, 0x27 is apostrophe (single quote)

# have to exclude numbers, otherwise the mix of ints and enums will flip things to NA

# lf is 0xa. exclude that
# newline (cr) is 0xd
# hive separator is 0x1?
# semicolon ..h2o apparently can auto-detect as separator. so don't use it.
# https://0xdata.atlassian.net/browse/HEX-1951

# hive separator is 0xa? ..down in the control chars I think
# tab is 0x9, so that's excluded

UTF16 = False
UTF8 = True
UTF8_MULTIBYTE = True

if UTF8:
    # what about multi-byte UTF8
    ordinalChoices = range(0x0, 0x100) # doesn't include last value ..allow ff
else:  # ascii subset?
    ordinalChoices = range(0x0, 0x80) # doesn't include last value ..allow 7f

if UTF8_MULTIBYTE:
    # 000000 - 00007f 1byte
    # 000080 - 00009f 2byte
    # 0000a0 - 0003ff 2byte
    # 000400 - 0007ff 2byte
    # 000800 - 003fff 3byte
    # 004000 - 00ffff 3byte
    # 010000 - 03ffff 3byte
    # 040000 - 10ffff 4byte
    # add some UTF8 multibyte, and restrict the choices to make sure we hit these
    ordinalChoices  = range(0x000000,0x00007f) # 1byte
    ordinalChoices += range(0x000080,0x00009f) # 2byte
    ordinalChoices += range(0x0000a0,0x0003ff) # 2byte
    ordinalChoices += range(0x000400,0x0007ff) # 2byte
    ordinalChoices += range(0x000800,0x003fff) # 3byte
    ordinalChoices += range(0x004000,0x00ffff) # 3byte
    ordinalChoices += range(0x010000,0x03ffff) # 3byte
    ordinalChoices += range(0x040000,0x10ffff) # 4byte

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
    # want to handle more than 256 numbers
    cList = []
    for i in range(length):
        # to go from hex 'string" to number
        # cint = int('fd9b', 16)
        r = random.choice(ordinalChoices)
        c = unichr(r).encode('utf-8')
        cList.append(c)
        print 

    # this is a random byte string now, of type string?
    return "".join(cList)

# Python details
# The rules for converting a Unicode string into the ASCII encoding are simple; for each code point:
#     If the code point is < 128, each byte is the same as the value of the code point.
#     If the code point is 128 or greater the Unicode string can't be represented in this encoding.
# UTF-8 uses the following rules:
#
#    If the code point is <128, it's represented by the corresponding byte value.
#    If the code point is between 128 and 0x7ff, it's turned into two byte values between 128 and 255.
#    Code points >0x7ff are turned into three- or four-byte sequences, where each byte of the sequence is between 128 and 255.


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
            # u = unichr(233) + unichr(0x0bf2) + unichr(3972) + unichr(6000) + unichr(13231)

            # left and right single quotes
            # u = unichr(0x201c) + unichr(0x201d)

            # preferred apostrophe (right single quote)
            # u = unichr(0x2019) 
            u = unichr(0x2018) + unichr(6000) + unichr(0x2019)

            # grave and acute?
            # u = unichr(0x60) + unichr(0xb4)
            # don't do this. grave with apostrophe http://www.cl.cam.ac.uk/~mgk25/ucs/quotes.html
            # u = unichr(0x60) + unichr(0x27)
            rowDataCsv = u
        else: # both ascii and utf-8 go here?
            rowData = []
            for j in range(colCount):
                r = generate_random_utf8_string(length=1)
                rowData.append(r)
            rowDataCsv = ",".join(rowData)

        if UTF16:
            # we're already passing it unicode. no decoding needed
            print "utf16:", repr(rowDataCsv), type(rowDataCsv)
            decoded = rowDataCsv
        else:
            print "str:", repr(rowDataCsv), type(rowDataCsv)
            # decoded = rowDataCsv.decode('utf-8')
            # decode to unicode
            decoded = rowDataCsv.decode('utf-8')
            # this has the right length..multibyte utf8 are decoded 
            print "utf8:" , repr(decoded), type(decoded)
        
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

    def test_parse_rand_utf8_multibyte(self):
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
