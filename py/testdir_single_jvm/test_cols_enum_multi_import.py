import unittest
import random, sys, time, os
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import as h2i

# the shared exec expression creator and executor
import h2o_exec as h2e

def write_syn_dataset(csvPathname, rowCount, colCount, SEED, translateList):
    # 8 random generatators, 1 per column
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
    print csvPathname


class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = random.randint(0, sys.maxint)

        # SEED = 
        random.seed(SEED)
        print "\nUsing random seed:", SEED
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(1,java_heap_GB=14)
        else:
            h2o_hosts.build_cloud_with_hosts()


    @classmethod
    def tearDownClass(cls):
        ## time.sleep(3600)
        h2o.tear_down_cloud()


    def test_many_cols_with_syn(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        translateList = ['a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u']
        tryList = [
            (300, 100, 'cA', 60),
            ]

        ### h2b.browseTheCloud()

        h2b.browseTheCloud()
        cnum = 0
        for (rowCount, colCount, key2, timeoutSecs) in tryList:
            cnum += 1
            # create 100 files with the same constraints 
            # FIX! should we add a header to them randomly???
            for fileN in range(500):
                rowxcol = str(rowCount) + 'x' + str(colCount)
                csvFilename = 'syn_' + str(fileN) + \
                    "_" + str(SEED) + "_" + rowxcol + '.csv'
                csvPathname = SYNDATASETS_DIR + '/' + csvFilename
                write_syn_dataset(csvPathname, rowCount, colCount, SEED, translateList)

            # DON"T get redireted to S3! (EC2 hack in config, remember!)
            # use it at the node level directly (because we gen'ed the files.
            h2o.nodes[0].import_files(SYNDATASETS_DIR)
            # use regex. the only files in the dir will be the ones we just created with  *fileN* match
            parseKey = h2o.nodes[0].parse('*'+rowxcol+'*', key2=key2, exclude=None, header=1, timeoutSecs=timeoutSecs)
            ### parseKey = h2o.nodes[0].parse('*49*', key2=key2, exclude=None, header=1, timeoutSecs=30)
            print "parseKey['destination_key']: " + parseKey['destination_key']
            print 'parse time:', parseKey['response']['time']

            inspect = h2o_cmd.runInspect(None, parseKey['destination_key'])

            # FIX! h2o strips one of the headers, but treats all the other files
            # with headers as data
            print "\n" + parseKey['destination_key'] + ":", \
                "    num_rows:", "{:,}".format(inspect['num_rows']), \
                "    num_cols:", "{:,}".format(inspect['num_cols'])


if __name__ == '__main__':
    h2o.unit_main()
