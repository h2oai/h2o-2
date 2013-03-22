import os, json, unittest, time, shutil, sys
# not needed, but in case you move it down to subdir
sys.path.extend(['.','..'])
import h2o_cmd
import h2o
import h2o_browse as h2b

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.build_cloud(node_count=3)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_A_Basic(self):
        h2o.verify_cloud_size()

    def test_B_RF_iris2(self):
        h2o_cmd.runRF(trees=6, timeoutSecs=10,
                csvPathname = h2o.find_file('smalldata/iris/iris2.csv'))

    def test_C_RF_poker100(self):
        h2o_cmd.runRF(trees=6, timeoutSecs=10,
                csvPathname = h2o.find_file('smalldata/poker/poker100'))

    def test_D_GenParity1(self):
        trees = 50
        h2o_cmd.runRF(trees=50, timeoutSecs=15, 
                csvPathname = h2o.find_file('smalldata/parity_128_4_100_quad.data'))

    def test_E_ParseManyCols(self):
        csvPathname=h2o.find_file('smalldata/fail1_100x11000.csv.gz')
        parseKey = h2o_cmd.parseFile(None, csvPathname, timeoutSecs=10)
        inspect = h2o_cmd.runInspect(None, parseKey['destination_key'], offset=-1, view=5)

    def test_F_StoreView(self):
        storeView = h2o.nodes[0].store_view()

    def test_G_Slower_JUNIT(self):
        h2o.tear_down_cloud()
        h2o.build_cloud(node_count=3)
        # we don't have the port or ip configuration here
        # that util/h2o.py does? Keep this in synch with spawn_h2o there.
        # also don't have --nosigar here?
        (ps, stdout, stderr) = h2o.spawn_cmd('junit', [
                'java',
                '-Dh2o.arg.ice_root='+h2o.tmp_dir('ice.'),
                '-Dh2o.arg.name='+h2o.cloud_name(),
                '-Dh2o.arg.ip='+h2o.get_ip_address(),
                '-ea', '-jar', h2o.find_file('target/h2o.jar'),
                '-mainClass', 'org.junit.runner.JUnitCore',
                # The tests
                'water.ConcurrentKeyTest',
                'hex.MinorityClassTest',
                'water.exec.RBigDataTest',
                ])
        rc = ps.wait(None)
        out = file(stdout).read()
        err = file(stderr).read()
        if rc is None:
            ps.terminate()
            raise Exception("junit timed out.\nstdout:\n%s\n\nstderr:\n%s" % (out, err))
        elif rc != 0:
            raise Exception("junit failed.\nstdout:\n%s\n\nstderr:\n%s" % (out, err))

if __name__ == '__main__':
    h2o.unit_main()
