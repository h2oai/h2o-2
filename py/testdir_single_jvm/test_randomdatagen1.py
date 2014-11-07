import unittest, re, os, sys
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_import as h2i

# test some random csv data, and some lineend combinations
class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init()
        global SYNDATASETS_DIR
        SYNDATASETS_DIR = h2o.make_syn_dir()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_A_randomdata2(self):
        print "Using datagen1.csv as-is"
        csvPathname = 'datagen1.csv'
        # have to give the separator == comma...otherwise H2O can't deduce it on this dataset
        parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, schema='put',
            timeoutSecs=10, header=0, separator=44)

        inspect = h2o_cmd.runInspect(key=parseResult['destination_key'])
        numCols = inspect['numCols']
        h2o_cmd.runRF(parseResult=parseResult, 
            trees=1, 
            response='C'+str(numCols),
            timeoutSecs=20)

    def test_B_randomdata2_1_lineend(self):
        csvPathname = 'datagen1.csv'
        # change lineend, case 1
        csvPathname1 = h2i.find_folder_and_filename('smalldata', csvPathname, returnFullPath=True)
        print "Using datagen1.csv to create", SYNDATASETS_DIR, "/datagen1.csv with different line ending" 
        csvPathname2 = SYNDATASETS_DIR + '/datagen1_crlf.csv'

        infile = open(csvPathname1, 'r') 
        outfile = open(csvPathname2,'w') # existing file gets erased

        # assume all the test files are unix lineend. 
        # I guess there shouldn't be any "in-between" ones
        # okay if they change I guess.
        for line in infile.readlines():
            outfile.write(line.strip("\n") + "\r")
        infile.close()
        outfile.close()

        parseResult = h2i.import_parse(path=csvPathname2, schema='put', 
            timeoutSecs=10, header=0, separator=44)

        inspect = h2o_cmd.runInspect(key=parseResult['destination_key'])
        numCols = inspect['numCols']
        h2o_cmd.runRF(parseResult=parseResult, 
            trees=1, 
            response='C'+str(numCols),
            timeoutSecs=20)


if __name__ == '__main__':
    h2o.unit_main()
