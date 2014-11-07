import unittest, time, sys, random 
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_glm, h2o_import as h2i, h2o_jobs, h2o_exec as h2e

DO_POLL = False
class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init(java_heap_GB=4)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_nfold_frame_extract(self):

        csvFilename = 'covtype.data'
        csvPathname = 'standard/' + csvFilename
        hex_key = "covtype.hex"

        parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, hex_key=hex_key, schema='local', timeoutSecs=30)

        print "Just nfold_frame_extract away and see if anything blows up"
        splitMe = hex_key
        inspect = h2o_cmd.runInspect(key=splitMe)
        origNumRows = inspect['numRows']
        origNumCols = inspect['numCols']
        for s in range(20):
            inspect = h2o_cmd.runInspect(key=splitMe)
            numRows = inspect['numRows']
            numCols = inspect['numCols']

            # FIX! should check if afold is outside of nfold range allowance
            fs = h2o.nodes[0].nfold_frame_extract(source=splitMe, nfolds=2, afold=random.randint(0,1))
            print "fs", h2o.dump_json(fs)

            split0_key = fs['split_keys'][0]
            split1_key = fs['split_keys'][1]
            split0_rows = fs['split_rows'][0]
            split1_rows = fs['split_rows'][1]
            print "Iteration", s, "split0_rows:", split0_rows, "split1_rows:", split1_rows
            splitMe = split0_key
            if split0_rows<=2:
                break

            print "Iteration", s

if __name__ == '__main__':
    h2o.unit_main()
