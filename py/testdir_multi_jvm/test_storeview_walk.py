import unittest, time, random, sys
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        # assume we're at 0xdata with it's hdfs namenode
        h2o.init(3)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_storeview_walk(self):
        print "Walk storeview using offset, and just view=1"
        print "Do an import to get keys"
        # importFolderPath = "/home/0xdiag/datasets/manyfiles-nflx-gz"
        # importFolderPath = "more1_1200_link"
        # FIX! storeview is really slow looking at the 1201 files in more1_1200_link
        if 1==0:
            MAX_FILE_NUM = 1201
            importFolderPath = "more1_1200_link"
        else:
            MAX_FILE_NUM = 385
            importFolderPath = "manyfiles-nflx-gz"

        # IMPORT**********************************************
        csvPathname = importFolderPath + "/*"
        (importFolderResult, importPattern) = h2i.import_only(bucket='home-0xdiag-datasets', path=csvPathname)
        # the list could be from hdfs/s3 (ec2 remap) or local. They have to different list structures
        if 'succeeded' in importFolderResult:
            succeededList = importFolderResult['succeeded']
        elif 'files' in importFolderResult:
            succeededList = importFolderResult['files']
        else:
            raise Exception ("Can't find 'files' or 'succeeded' in import list")

        ### print "succeededList:", h2o.dump_json(succeededList)
        print len(succeededList), "keys reported by import result"
        ### self.assertEqual(len(succeededList), MAX_FILE_NUM, "There should be %s files imported as keys" % MAX_FILE_NUM)

        print "\nTrying StoreView after the import folder"
        # look at all? How do I know how many there are?
        # have to override the default 1024 in h2o.py store_view
        storeViewResult = h2o_cmd.runStoreView(offset=0, view=None, timeoutSecs=30)
        l = len(storeViewResult['keys'])
        print "Check 1:", l, "%s keys if view= not used" % l
        self.assertEqual(20, l, "Expect 20 (default) for view= not used")

        # can't handle MAX_FILE_NUM in storeView. max is 1024
        storeViewResult = h2o_cmd.runStoreView(offset=0, view=1024, timeoutSecs=60)
        l = len(storeViewResult['keys'])
        ### print "Check 2:", l, "%s keys if view=1024" % l
        ### self.assertEqual(MAX_FILE_NUM, l, "Expect 1024 for view=1024")

        storeViewResult = h2o_cmd.runStoreView(offset=0, view=0, timeoutSecs=60)
        l = len(storeViewResult['keys'])
        print "Check 3:", l, "%s keys if view= 0" % l
        self.assertEqual(0, l, "Expect 0 for view=0")

        storeViewResult = h2o_cmd.runStoreView(offset=0, view=MAX_FILE_NUM, timeoutSecs=60)
        l = len(storeViewResult['keys'])
        ### print "Check 4:", l, "%s keys if view= number of files in the dir", MAX_FILE_NUM
        ### self.assertEqual(MAX_FILE_NUM, l, "Expect %s for view=%s" % (MAX_FILE_NUM, MAX_FILE_NUM))

        keys = storeViewResult['keys']
        # look at one at a time
        i = 0
        while i < len(keys):
            h2o_cmd.runStoreView(timeoutSecs=30, offset=i, view=2)
            i += 1

        # look at two at a time
        i = 0
        while i < len(keys):
            h2o_cmd.runStoreView(timeoutSecs=30, offset=i, view=2)
            i += 2

        # FIX! should check the case of offset being bigger than the # of keys

if __name__ == '__main__':
    h2o.unit_main()
