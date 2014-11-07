import unittest
import random, sys, time, os
sys.path.extend(['.','..','../..','py'])

import h2o, h2o_cmd, h2o_import as h2i, h2o_exec as h2e
import codecs

# This shows the test really created a UTF8 file that was not a ASCII file
# ~/h2o/py/testdir_multi_jvm$ file sandbox/syn*/*
# sandbox/syn_datasets/syn_3234802987159914820_1000x1.csv: UTF-8 Unicode text
# sandbox/syn_datasets/syn_7454586956682649267_1000x1.csv: UTF-8 Unicode text
#sandbox/syn_datasets/syn_8233902282973358813_1000x1.csv: UTF-8 Unicode text

print 'Maybe h2o is supposed to NA rows if they start with an unmatched "'
print "For now, complain if numRows isn't what I generated"
print
print "We throw in double quotes to terminate the string randomly, along with random within the string"
print "Will this change row/col? not sure. failing on numRows right now"
print "allow for an extra col to be created relative to my one col expectation"

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
UTF8 = False
ASCII = True
UTF8_MULTIBYTE = False

DOUBLE_QUOTE = True
DOUBLE_QUOTE_END_RANDOM = True

SINGLE_QUOTE = True

if ASCII:
    # leave DEL (0x7F) out also, in addition to the control chars 0x0-0x1f)
    ordinalChoices = range(0x20, 0x7f) # doesn't include last value
elif UTF8:
    # what about multi-byte UTF8
    ordinalChoices = range(0x0, 0x100) # doesn't include last value
elif DOUBLE_QUOTE:
    # ascii subset
    ordinalChoices = range(0x0, 0x40) # doesn't include last value
else:  # ascii subset?
    ordinalChoices = range(0x0, 0x80) # doesn't include last value


if UTF8_MULTIBYTE:
    # add some UTF8 multibyte, and restrict the choices to make sure we hit these
    ordinalChoices = range(0x0, 0x40) # doesn't include last value ..allow 7f
    ordinalChoices += [0x201c, 0x201d, 0x2018, 0x2019, 6000]

def safe_remove(thing, thingList):
    # remove first occurence if in list
    if thing in thingList:
        thingList.remove(thing)

safe_remove(0x09, ordinalChoices) # is 9 bad..apparently can cause NA

safe_remove(0x00, ordinalChoices) # nul
safe_remove(0x0d, ordinalChoices) # cr
safe_remove(0x0a, ordinalChoices) # lf
safe_remove(0x01, ordinalChoices) # hiveseparator

# smaller range, avoiding 0-1f control chars
safe_remove(0x3b, ordinalChoices) # semicolon
safe_remove(0x20, ordinalChoices) # space
safe_remove(0x22, ordinalChoices) # double quote

if not SINGLE_QUOTE:
    safe_remove(0x27, ordinalChoices) # apostrophe. should be legal if single quotes not enabled
safe_remove(0x2c, ordinalChoices) # comma

safe_remove(0x30, ordinalChoices) # 0
safe_remove(0x31, ordinalChoices) # 1
safe_remove(0x32, ordinalChoices) # 2
safe_remove(0x33, ordinalChoices) # 3
safe_remove(0x34, ordinalChoices) # 4
safe_remove(0x35, ordinalChoices) # 5
safe_remove(0x36, ordinalChoices) # 6
safe_remove(0x37, ordinalChoices) # 7
safe_remove(0x38, ordinalChoices) # 8
safe_remove(0x39, ordinalChoices) # 9

if DOUBLE_QUOTE:
    # ascii subset
    ordinalChoices.append(0x22) # double quote

def generate_random_utf8_string(length=5):
        # want to handle more than 256 numbers
        cList = []
        for i in range(length):
            # to go from hex 'string" to number
            # cint = int('fd9b', 16)
            r = random.choice(ordinalChoices)
            c = unichr(r).encode('utf-8')
            cList.append(c)

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
                r = generate_random_utf8_string(length=5)
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
        
        # TEMP: always end every line with a double quote. This might complete some double quote starters
        # so that means the # of cols will be wrong? maybe allow it to be 1 more below
        # dsf.write(rowDataCsv + "\n")
        # maybe do it randomly
        if DOUBLE_QUOTE_END_RANDOM and (random.randint(0,1)==1): 
            dsf.write(decoded + '"' + "\n")
        else:
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

    def test_parse_rand_utf8_unmatched_double(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        tryList = [
            (1000, 1, 'cA', 120),
            (1000, 1, 'cG', 120),
            (1000, 1, 'cH', 120),
            ]

        print "What about messages to log (INFO) about unmatched quotes (before eol)"
        # got this ..trying to avoid for now
        # Exception: rjson error in parse: Argument 'source_key' error: Parser setup appears to be broken, got AUTO

        print "what we used"
        print "ordinalChoices:", ordinalChoices
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
            self.assertEqual(numRows, rowCount, msg='Wrong numRows likely due to unmatched " row going to NA: %s %s' % (numRows, rowCount))
            numCols = inspect['numCols']

            # because of our double quote termination hack above
            if DOUBLE_QUOTE:
                self.assertTrue((numCols==colCount or numCols==colCount+1), msg='Wrong numCols: %s %s' % (numCols, colCount))
            else:
                self.assertTrue(numCols==colCount, msg='Wrong numCols: %s %s' % (numCols, colCount))

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
