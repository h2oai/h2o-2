import unittest, random, sys
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_util, h2o_import as h2i
import h2o_exec as h2e

UNNECESSARY = False
class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init(java_heap_GB=24)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_big_sum_fail(self):
        node = h2o.nodes[0]
        SYNDATASETS_DIR = h2o.make_syn_dir()
        csvPathname = SYNDATASETS_DIR + '/temp.csv'
        hex_key = 'temp.hex'
        for trial in range(5):
            # what about seed?
            cfResult = h2o.nodes[0].create_frame(key=hex_key,
                binary_ones_fraction=0.02, binary_fraction=0, randomize=1, 
                missing_fraction=0, integer_fraction=1, real_range=100,
                has_response=0, response_factors=2, factors=100, cols=1, 
                integer_range=100, value=0, categorical_fraction=0, rows=2.5e+08, 
                timeoutSecs=300)

            inspect = h2o_cmd.runInspect(key=hex_key)
            h2o_cmd.infoFromInspect(inspect, hex_key)

            if UNNECESSARY:
                # this is just doing a head to R. not critical
                h2e.exec_expr(execExpr="%s = %s" % (hex_key, hex_key))
                h2e.exec_expr(execExpr="Last.value.0 = %s[c(1,2,3,4,5,6),]" % hex_key)
                h2e.exec_expr(execExpr="Last.value.0 = Last.value.0")
                node.csv_download(src_key="Last.value.0", csvPathname=csvPathname)
                node.remove_key("Last.value.0")
                # not sure why this happened
                h2o_cmd.runStoreView(view=10000, offset=0)


            # Fails on this
            h2e.exec_expr(execExpr='Last.value.1 = %s[,1]' % hex_key)

            print "Trial #", trial, "completed"

if __name__ == '__main__':
    h2o.unit_main()
