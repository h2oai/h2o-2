import unittest, time, sys
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_import as h2i, h2o_browse as h2b


def write_syn_dataset(csvPathname, rowCount, headerData, rowData):
    dsf = open(csvPathname, "w+")
    dsf.write(headerData + "\n")
    for i in range(rowCount):
        dsf.write(rowData + "\n")
    dsf.close()

# append!
def append_syn_dataset(csvPathname, rowData):
    with open(csvPathname, "a") as dsf:
        dsf.write(rowData + "\n")

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        # fails with 3
        h2o.init(3, java_heap_GB=4, use_flatfile=True)

        h2b.browseTheCloud()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud(h2o.nodes)
    
    def test_parse_fs_schmoo_fvec(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        csvFilename = "syn_prostate.csv"
        csvPathname = SYNDATASETS_DIR + '/' + csvFilename

        headerData = "ID,CAPSULE,AGE,RACE,DPROS,DCAPS,PSA,VOL,GLEASON"
        # rowData = "1,0,65,1,2,1,1.4,0,6"
        rowData = "1,0,65,1,2,1,1,0,6"

        totalRows = 99860
        write_syn_dataset(csvPathname, totalRows, headerData, rowData)

        print "This is the same format/data file used by test_same_parse, but the non-gzed version"
        print "\nSchmoo the # of rows"
        print "Updating the key and hex_key names for each trial"
        for trial in range (200):
            append_syn_dataset(csvPathname, rowData)
            totalRows += 1

            start = time.time()
            key = csvFilename + "_" + str(trial)
            hex_key = csvFilename + "_" + str(trial) + ".hex"
            parseResult = h2i.import_parse(path=csvPathname, schema='put', hex_key=hex_key)
            print "trial #", trial, "totalRows:", totalRows, "parse end on ", \
                csvFilename, 'took', time.time() - start, 'seconds'

            h2o_cmd.runInspect(key=hex_key)
            # only used this for debug to look at parse (red last row) on failure
            ### h2b.browseJsonHistoryAsUrlLastMatch("Inspect")
            h2o.check_sandbox_for_errors()

if __name__ == '__main__':
    h2o.unit_main()
