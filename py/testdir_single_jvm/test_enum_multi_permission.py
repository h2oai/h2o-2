import unittest, random, sys, time, os, stat, pwd, grp
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i, h2o_exec as h2e

FILENUM=100

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
        h2o.init(1,java_heap_GB=14)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_cols_multi_permission(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        translateList = ['a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u']
        tryList = [
            (300, 100, 'cA', 60),
            ]

        # h2b.browseTheCloud()
        cnum = 0
        for (rowCount, colCount, hex_key, timeoutSecs) in tryList:
            cnum += 1
            # FIX! should we add a header to them randomly???
            print "Wait while", FILENUM, "synthetic files are created in", SYNDATASETS_DIR
            rowxcol = str(rowCount) + 'x' + str(colCount)
            for fileN in range(FILENUM):
                csvFilename = 'syn_' + str(fileN) + "_" + str(SEED) + "_" + rowxcol + '.csv'
                csvPathname = SYNDATASETS_DIR + '/' + csvFilename
                write_syn_dataset(csvPathname, rowCount, colCount, SEED, translateList)

            # DON"T get redirected to S3! (EC2 hack in config, remember!)
            # use it at the node level directly (because we gen'ed the files.
            # use regex. the only files in the dir will be the ones we just created with  *fileN* match
            parseResult = h2i.import_parse(path=SYNDATASETS_DIR + '/*'+rowxcol+'*', schema='local',
                exclude=None, header=1, timeoutSecs=timeoutSecs)
            print "parseResult['destination_key']: " + parseResult['destination_key']
            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
            h2o_cmd.infoFromInspect(inspect, csvPathname)


            # FIX! h2o strips one of the headers, but treats all the other files with headers as data
            print "\n" + parseResult['destination_key'] + ":", \
                "    numRows:", "{:,}".format(inspect['numRows']), \
                "    numCols:", "{:,}".format(inspect['numCols'])

            # get uid/gid of files the test create (dir here)
            origUid = os.getuid()
            origGid = os.getgid()
            print "my uid and gid:", origUid, origGid

            # pick one file to flip
            fileList = os.listdir(SYNDATASETS_DIR)
            badFile = random.choice(fileList)
            badPathname = SYNDATASETS_DIR + "/" + badFile
            print "Going to use this file as the bad file:", badPathname

            print "checking os.chmod and parse"
            # os.chmod(badPathname, stat.S_IRWXU | stat.S_IRWXO)
            # always have to re-import because source key is deleted by h2o
            parseResult = h2i.import_parse(path=SYNDATASETS_DIR + '/*'+rowxcol+'*', schema='local',
                exclude=None, header=1, timeoutSecs=timeoutSecs)
            print "parseResult['destination_key']: " + parseResult['destination_key']
            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
            h2o_cmd.infoFromInspect(inspect, csvPathname)

            print "write by owner, only, and parse"
            os.chmod(badPathname, stat.S_IWRITE)
            parseResult = h2i.import_parse(path=SYNDATASETS_DIR + '/*'+rowxcol+'*', schema='local',
                exclude=None, header=1, timeoutSecs=timeoutSecs)
            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
            h2o_cmd.infoFromInspect(inspect, csvPathname)

            print "execute by owner, only, and parse"
            os.chmod(badPathname, stat.S_IEXEC)
            h2o.nodes[0].import_files(SYNDATASETS_DIR)
            parseResult = h2i.import_parse(path=SYNDATASETS_DIR + '/*'+rowxcol+'*', schema='local',
                exclude=None, header=1, timeoutSecs=timeoutSecs)
            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
            h2o_cmd.infoFromInspect(inspect, csvPathname)

            # change back to normal
            # os.chmod(badPathname, stat.S_IRWXU | stat.S_IRWXO)
            
            # how to make this work? disable for now
            if (1==0):
                # now change uid
                badUid = pwd.getpwnam("nobody").pw_uid
                badGid = grp.getgrnam("nogroup").gr_gid

                print "parsing after one bad uid"
                os.chown(badPathname, badUid, origGid)
                parseResult = h2i.import_parse(path=SYNDATASETS_DIR + '/*'+rowxcol+'*', schema='local',
                    exclude=None, header=1, timeoutSecs=timeoutSecs)
                print "parsing after one bad gid"
                os.chown(badPathname, origUid, badGid)
                parseResult = h2i.import_parse(path=SYNDATASETS_DIR + '/*'+rowxcol+'*', schema='local',
                    exclude=None, header=1, timeoutSecs=timeoutSecs)

                os.chown(badPathname, origUid, origGid)


if __name__ == '__main__':
    h2o.unit_main()
