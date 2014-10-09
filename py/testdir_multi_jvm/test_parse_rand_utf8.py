import unittest
import random, sys, time, os
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts, h2o_import as h2i, h2o_exec as h2e

print "apparently need to have at least one normal character otherwise the parse doesn't work right"
print "is char 0x00 treated as NA? skip"

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
nonQuoteChoices = range(0x0, 0x80) # doesn't include last value ..allow 7f

nonQuoteChoices.remove(0x09) # is 9 bad

nonQuoteChoices.remove(0x00) # nul
nonQuoteChoices.remove(0x0d) # cr
nonQuoteChoices.remove(0x0a) # lf
nonQuoteChoices.remove(0x01) # hiveseparator

# smaller range, avoiding 0-1f control chars
# nonQuoteChoices = range(0x20, 0x7f) # doesn't include last value
nonQuoteChoices.remove(0x3b) # semicolon
nonQuoteChoices.remove(0x20) # space
nonQuoteChoices.remove(0x22) # double quote
# nonQuoteChoices.remove(0x27) # apostrophe. should be legal if single quotes not enabled
nonQuoteChoices.remove(0x2c) # comma

nonQuoteChoices.remove(0x30) # 0
nonQuoteChoices.remove(0x31) # 1
nonQuoteChoices.remove(0x32) # 2
nonQuoteChoices.remove(0x33) # 3
nonQuoteChoices.remove(0x34) # 4
nonQuoteChoices.remove(0x35) # 5
nonQuoteChoices.remove(0x36) # 6
nonQuoteChoices.remove(0x37) # 7
nonQuoteChoices.remove(0x38) # 8
nonQuoteChoices.remove(0x39) # 9
# print nonQuoteChoices

def generate_random_utf8_string(length=1):
    return "".join(unichr(random.choice(nonQuoteChoices)) for i in range(length-1))

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
    dsf = open(csvPathname, "w+")
    for i in range(rowCount):
        rowData = []
        for j in range(colCount):
            r = generate_random_utf8_string(length=2)
            rowData.append(r)

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
            h2o.build_cloud(2,java_heap_GB=1,use_flatfile=True)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_parse_rand_utf8(self):
        h2o.beta_features = True
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
        u = unichr(233) + unichr(0x0bf2) + unichr(3972) + unichr(6000) + unichr(13231)

        for i, c in enumerate(u):
            print i, '%04x' % ord(c), unicodedata.category(c),
            print unicodedata.name(c)

        # Get numeric value of second character
        print unicodedata.numeric(u[1])
        #**************************

if __name__ == '__main__':
    h2o.unit_main()
