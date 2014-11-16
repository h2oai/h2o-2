import unittest, random, sys, time, os
sys.path.extend(['.','..','../..','py'])

import h2o, h2o_cmd, h2o_import as h2i
import codecs, unicodedata
print "create some specific small datasets with exp row/col combinations"
print "This is trying a random sample of utf8 symbols inserted in an enum"

# 0x1 can be the hive separator? if we force comma it should be treated as char
# should try without and change expected cols

# the full list of legal UTF8
toDoList  = range(0x000000,0x00007f) # 1byte
toDoList += range(0x000080,0x00009f) # 2byte
toDoList += range(0x0000a0,0x0003ff) # 2byte
toDoList += range(0x000400,0x0007ff) # 2byte
toDoList += range(0x000800,0x003fff) # 3byte
toDoList += range(0x004000,0x00ffff) # 3byte
toDoList += range(0x010000,0x03ffff) # 3byte
toDoList += range(0x040000,0x10ffff) # 4byte

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
# do just 100
for i in random.sample(toDoList, 100):
    # ignore illegal 
    try: 
        unicodeSymbol = unichr(i)
        tryList.append(
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
    except ValueError: 
        # for now, we don't expect any illegal values in toDoList
        raise Exception("0x%x isn't valid unicode point" % i)
        

def write_syn_dataset(csvPathname, dataset):
    dsf = codecs.open(csvPathname, encoding='utf-8', mode='w+')
    encoded = dataset.encode('utf-8')
    # print "utf8:" , repr(encoded), type(encoded)
    # print "str or utf8:" , repr(dataset), type(dataset)
    dsf.write(dataset)
    dsf.close()

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init(java_heap_GB=1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_parse_specific_case5(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        hex_key = "a.hex"

        for (dataset, expNumRows, expNumCols, expNaCnt, expType, unicodeNum) in tryList:
            csvFilename = 'specific_' + str(expNumRows) + str(expNumCols) + '.csv'
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename
            write_syn_dataset(csvPathname, dataset)

            parseResult = h2i.import_parse(path=csvPathname, schema='put', header=0,
                # force col separator
                hex_key=hex_key, timeoutSecs=10, doSummary=False, separator=H2O_COL_SEPARATOR) 
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
