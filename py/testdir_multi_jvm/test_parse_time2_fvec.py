import random, unittest, time, sys
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i


ROW_VALUES = False
# some dates are "wrong"..i.e. the date should be constrained
# depending on month and year.. Assume 1-31 is legal
months = [
    ['Jan', 'JAN'],
    ['Feb', 'FEB'],
    ['Mar', 'MAR'],
    ['Apr', 'APR'],
    ['May', 'MAY'],
    ['Jun', 'JUN'],
    ['Jul', 'JUL'],
    ['Aug', 'AUG'],
    ['Sep', 'SEP'],
    ['Oct', 'OCT'],
    ['Nov', 'NOV'],
    ['Dec', 'DEC']
    ]

def getRandomDate():
    # assume leading zero is option
    day = str(random.randint(1,31)).zfill(2)
    if random.randint(0,1) == 1:
        day = day.zfill(2) 

    year = str(random.randint(0,99)).zfill(2)
    if random.randint(0,1) == 1:
        year = year.zfill(2) 

    # randomly decide on number or translation for month
    ### if random.randint(0,1) == 1:
    # FIX! H2O currently only supports the translate months
    if 1==1:
        month = random.randint(1,12)
        monthTranslateChoices = months[month-1]
        month = random.choice(monthTranslateChoices)
    else:
        month = str(random.randint(1,12)).zfill(2)
        if random.randint(0,1) == 1:
            month = month.zfill(2) 

    a  = "%s-%s-%s" % (day, month, year)
    return a

def getMinDate():
    a  = "%s-%s-%s" % (00,"Jan",00)
    return a

def getMaxDate():
    # a  = "%s-%s-%s" % (31,"Dec",99)
    a  = "%s-%s-%s" % (31,"Dec",20)
    return a

def rand_rowData(colCount=6):
    a = [getRandomDate() for fields in range(colCount)]
    # put a little white space in!
    b = ", ".join(map(str,a))
    return b

def max_rowData(colCount=6):
    a = [getMaxDate() for fields in range(colCount)]
    b = ", ".join(map(str,a))

def min_rowData(colCount=6):
    a = [getMinDate() for fields in range(colCount)]
    b = ", ".join(map(str,a))

def write_syn_dataset(csvPathname, rowCount, headerData=None, rowData=None):
    dsf = open(csvPathname, "w+")
    if headerData is not None:
        dsf.write(headerData + "\n")
    if rowData is not None:
        for i in range(rowCount):
            dsf.write(rowData + "\n")
    dsf.close()

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init(2,java_heap_GB=10,use_flatfile=True)
        ## h2b.browseTheCloud()

    @classmethod
    def tearDownClass(cls):
        ### time.sleep(3600)
        h2o.tear_down_cloud(h2o.nodes)
    
    def test_parse_time2_fvec(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        csvFilename = "syn_time.csv"
        csvPathname = SYNDATASETS_DIR + '/' + csvFilename

        headerData = None
        colCount = 12
        rowData = rand_rowData(colCount)
        rowCount = 3
        write_syn_dataset(csvPathname, rowCount, headerData, rowData)

        for trial in range (20):
            if trial==0:
                rowData = min_rowData()
            elif trial==1:
                rowData = max_rowData()
            else: 
                rowData = rand_rowData()

            # make sure all key names are unique, when we re-put and re-parse (h2o caching issues)
            src_key = csvFilename + "_" + str(trial)
            hex_key = csvFilename + "_" + str(trial) + ".hex"

            start = time.time()
            parseResultA = h2i.import_parse(path=csvPathname, schema='put', src_key=src_key, hex_key=hex_key)
            print "\nA trial #", trial, "parse end on ", csvFilename, 'took', time.time() - start, 'seconds'

            inspect = h2o_cmd.runInspect(key=hex_key)
            ### print h2o.dump_json(inspect)

            missingValuesListA = h2o_cmd.infoFromInspect(inspect, csvPathname)
            print "missingValuesListA", missingValuesListA

            numColsA = inspect['numCols']
            numRowsA = inspect['numRows']

            self.assertEqual(missingValuesListA, [], "missingValuesList should be empty")
            self.assertEqual(numColsA, colCount)
            self.assertEqual(numRowsA, rowCount)

            # FIX! can't see to get row values any more?
            if ROW_VALUES:
                for r in inspect['rows']:
                    for c in r: # cols
                        k = r[c]
                        # ignore the "row" entry ..it's a row number
                        # hex-739 addresses this problem (if a column has name "row")..probably want a ordered list for rows, not dictionary
                        if c != "row":
                            if k < 959238000000:
                                raise Exception ("row: %s col: %s value: %s is too small for date" % (r, c, k))
                            if k > 4089855600000:
                                raise Exception ("row: %s col: %s value: %s is too big for date" % (r, c, k))

            ### h2b.browseJsonHistoryAsUrlLastMatch("Inspect")
            h2o.check_sandbox_for_errors()

if __name__ == '__main__':
    h2o.unit_main()

    


