import unittest, random, sys, time, os, stat, pwd, grp
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i, h2o_exec as h2e

FILENUM=2

def write_syn_dataset(csvPathname, rowCount, colCount, SEED, translateList):
    r1 = random.Random(SEED)
    dsf = open(csvPathname, "w+")
    roll = random.randint(0,1)
    # if roll==0:
    if 1==1:
        # spit out a header
        rowData = []
        for j in range(colCount):
            rowData.append('h' + str(j))

        rowDataCsv = ",".join(map(str,rowData))
        dsf.write(rowDataCsv + "\n")

    for i in range(rowCount):
        rowData = []
        for j in range(colCount):
            ri1 = r1.triangular(0,3,1.5)
            ri1Int = int(round(ri1,0))
            rowData.append(ri1Int)

        if translateList is not None:
            for i, iNum in enumerate(rowData):
                rowData[i] = translateList[iNum]

        rowDataCsv = ",".join(map(str,rowData))
        dsf.write(rowDataCsv + "\n")

    dsf.close()
    # print csvPathname

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        print "WARNING: won't work for remote h2o, because syn_datasets is created locally only, for import"
        h2o.init(java_heap_GB=14)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_parse_multi_exclude_fvec(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        translateList = ['a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u']
        tryList = [
            (300, 100, 'cA', 60, '*x[2-5]*'),
            (310, 200, 'cB', 60, '*x[1,3-5]*'),
            (320, 300, 'cC', 60, '*x[1-2,4-5]*'),
            (330, 400, 'cD', 60, '*x[1-3-5]*'),
            (340, 500, 'cE', 60, '*x[1-4]*'),
            ]

        ## h2b.browseTheCloud()
        cnum = 0
        # create them all first
        for (rowCount, colCount, hex_key, timeoutSecs, excludePattern) in tryList:
            cnum += 1
            # FIX! should we add a header to them randomly???
            print "Wait while", FILENUM, "synthetic files are created in", SYNDATASETS_DIR
            rowxcol = str(rowCount) + 'x' + str(colCount)
            for fileN in range(FILENUM):
                csvFilename = 'syn_' + str(fileN) + "_" + str(SEED) + "_" + rowxcol + '.csv'
                csvPathname = SYNDATASETS_DIR + '/' + csvFilename
                write_syn_dataset(csvPathname, rowCount, colCount, SEED, translateList)

        for (rowCount, colCount, hex_key, timeoutSecs, excludePattern) in tryList:
            cnum += 1
            # put them, rather than using import files, so this works if remote h2o is used
            # and python creates the files locally
            fileList = os.listdir(SYNDATASETS_DIR)
            for f in fileList:
                print f
                h2i.import_only(path=SYNDATASETS_DIR + "/" + f, schema='put')

            # pattern match all, then use exclude
            parseResult = h2i.parse_only(pattern="*syn_*",
                hex_key=hex_key, exclude=excludePattern, header=1, timeoutSecs=timeoutSecs)
            print "parseResult['destination_key']: " + parseResult['destination_key']

            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
            h2o_cmd.infoFromInspect(inspect, csvPathname)


            # FIX! h2o strips one of the headers, but treats all the other files with headers as data
            numRows = inspect['numRows']
            numCols = inspect['numCols']
            print "\n" + parseResult['destination_key'] + ":", \
                "    numRows:", "{:,}".format(numRows), \
                "    numCols:", "{:,}".format(numCols)

            # all should have rowCount rows (due to the excludePattern
            self.assertEqual(numRows, rowCount*FILENUM, msg=("got numRows: %s. Should be rowCount: %s * FILENUM: %s" % \
                (numRows, rowCount, FILENUM)))

if __name__ == '__main__':
    h2o.unit_main()
