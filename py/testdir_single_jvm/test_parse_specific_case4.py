import unittest, random, sys, time, os
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts, h2o_import as h2i
import codecs, unicodedata
print "create some specific small datasets with exp row/col combinations"
print "I'll keep it to one case per file"

# toDoList = range(0x0,0x80)
# 0x1 can be the hive separator? if we force comma it should be treated as char
# should try without and change expected cols
toDoList = range(0x00, 0x100)

def removeIfThere(d):
    if d in toDoList:
        toDoList.remove(d)

H2O_COL_SEPARATOR = 0x2c # comma
# H2O_COL_SEPARATOR = 0x1 # hive separator

# removeIfThere(0x1) # hive separator okay if we force comma below

removeIfThere(0x0) # nul. known issue
removeIfThere(0xa) # LF. causes EOL
removeIfThere(0xd) # CR. causes EOL
removeIfThere(0x22) # double quote. known issue
removeIfThere(0x2c) # comma. don't mess up my expected col count

# could try single quote if enabled, to see if does damage. probably like double quote

tryList = []
for i in toDoList:
    unicodeSymbol = unichr(i)

    tryList.append(
        # the nul char I think is causing extra rows and also wiping out the next char?
        # I got nulls when concat'ing files with dd. may be used for padding somehow?
        ((
        'a,b,c,d' + unicodeSymbol + 's,n\n'
        'a,b,c,d' + unicodeSymbol + 's,n\n'
        'a,b,c,d' + unicodeSymbol + 's,n\n'
        'a,b,c,d' + unicodeSymbol + 's,n\n'
        'a,b,c,d' + unicodeSymbol + 's,n\n'
        'a,b,c,d' + unicodeSymbol + 's,n\n'
        'a,b,c,d' + unicodeSymbol + 's,n\n'
        'a,b,c,d' + unicodeSymbol + 's,n\n'
        'a,b,c,d' + unicodeSymbol + 's,n\n'
        'a,b,c,d' + unicodeSymbol + 's,n\n'
        ), 10, 5, [0,0,0,0,0], ['Enum', 'Enum', 'Enum', 'Enum', 'Enum'], i)
    )

def write_syn_dataset(csvPathname, dataset):
    dsf = codecs.open(csvPathname, encoding='utf-8', mode='w+')
    encoded = dataset.encode('utf-8')
    print "utf8:" , repr(encoded), type(encoded)
    print "str or utf8:" , repr(dataset), type(dataset)
    dsf.write(dataset)
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
            h2o.build_cloud(java_heap_GB=1)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_parse_specific_case4(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        hex_key = "a.hex"

        for (dataset, expNumRows, expNumCols, expNaCnt, expType, unicodeNum) in tryList:
            csvFilename = 'specific_' + str(expNumRows) + str(expNumCols) + '.csv'
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename
            write_syn_dataset(csvPathname, dataset)

            parseResult = h2i.import_parse(path=csvPathname, schema='put', header=0,
                hex_key=hex_key, timeoutSecs=10, doSummary=False, separator=H2O_COL_SEPARATOR) # force comma separator
            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'], timeoutSecs=60)
            
            print "Parsed with special unichr(%s) which is %s:" % (unicodeNum, unichr(unicodeNum))
            # print "inspect:", h2o.dump_json(inspect)
            numRows = inspect['numRows']
            self.assertEqual(numRows, expNumRows, msg='Using unichr(0x%x) Wrong numRows: %s Expected: %s' % \
                (unicodeNum, numRows, expNumRows))
            numCols = inspect['numCols']
            self.assertEqual(numCols, expNumCols, msg='Using unichr(0x%x) Wrong numCols: %s Expected: %s' % \
                (unicodeNum, numCols, expNumCols))

            # this is required for the test setup
            assert(len(expNaCnt)>=expNumCols)
            assert(len(expType)>=expNumCols)

            for k in range(expNumCols):
                naCnt = inspect['cols'][k]['naCnt']
                self.assertEqual(expNaCnt[k], naCnt, msg='Using unichr(0x%x) col: %s naCnt: %d should be: %s' % \
                    (unicodeNum, k, naCnt, expNaCnt[k]))
                stype = inspect['cols'][k]['type']
                self.assertEqual(expType[k], stype, msg='Using unichr(0x%x) col: %s type: %s should be: %s' % \
                    (unicodeNum, k, stype, expType[k]))

if __name__ == '__main__':
    h2o.unit_main()
