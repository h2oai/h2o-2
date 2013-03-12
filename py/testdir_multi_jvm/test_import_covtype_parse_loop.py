import unittest, sys, random
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.build_cloud(node_count=3)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_import_covtype_parse_loop(self):

        print "put file 'covtype.data' once, then loop parsing to unique keys"
        csvFilename = "covtype.data"
        importFolderPath = "/home/0xdiag/datasets"
        h2i.setupImportFolder(None, importFolderPath)

        lenNodes = len(h2o.nodes)
        trialMax = 20
        for trial in range(trialMax):
            key2 = csvFilename + "_" + str(trial) + ".hex"
            nodeX = random.randint(0,lenNodes-1) 
            parseKey = h2i.parseImportFolderFile(h2o.nodes[nodeX], csvFilename, importFolderPath, key2=key2, timeoutSecs=20)
            sys.stdout.write('.')
            sys.stdout.flush()

if __name__ == '__main__':
    h2o.unit_main()
