import os, json, unittest, time, shutil, sys
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts
import h2o_browse as h2b


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

class glm_same_parse(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        # fails with 3
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(3,java_heap_GB=4,use_flatfile=True)
        else:
            h2o_hosts.build_cloud_with_hosts()

        h2b.browseTheCloud()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud(h2o.nodes)
    
    def test_sort_of_prostate_with_row_schmoo(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        csvFilename = "syn_prostate.csv"
        csvPathname = SYNDATASETS_DIR + '/' + csvFilename

        headerData = "ID,CAPSULE,AGE,RACE,DPROS,DCAPS,PSA,VOL,GLEASON"
        rowData = "1,0,65,1,2,1,1.4,0,6"

        totalRows = 99860
        write_syn_dataset(csvPathname, totalRows, headerData, rowData)

        print "This is the same format/data file used by test_same_parse, but the non-gzed version"
        print "\nSchmoo the # of rows"
        print "Updating the key and key2 names for each trial"
        for trial in range (200):
            append_syn_dataset(csvPathname, rowData)
            totalRows += 1
            ### start = time.time()
            # this was useful to cause failures early on. Not needed eventually
            ### key = h2o_cmd.parseFile(csvPathname=h2o.find_file("smalldata/logreg/prostate.csv"))
            ### print "Trial #", trial, "parse end on ", "prostate.csv" , 'took', time.time() - start, 'seconds'

            start = time.time()
            key = csvFilename + "_" + str(trial)
            key2 = csvFilename + "_" + str(trial) + ".hex"
            key = h2o_cmd.parseFile(csvPathname=csvPathname, key=key, key2=key2)
            print "trial #", trial, "totalRows:", totalRows, "parse end on ", \
                csvFilename, 'took', time.time() - start, 'seconds'

            h2o_cmd.runInspect(key=key2)
            # only used this for debug to look at parse (red last row) on failure
            ### h2b.browseJsonHistoryAsUrlLastMatch("Inspect")
            h2o.check_sandbox_for_errors()

if __name__ == '__main__':
    h2o.unit_main()
