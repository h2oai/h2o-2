import unittest, time, sys
# not needed, but in case you move it down to subdir
sys.path.extend(['.','..'])
import h2o, h2o_cmd, h2o_import2 as h2i
import h2o_browse as h2b

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.build_cloud(node_count=3,java_heap_GB=1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_A_Basic(self):
        h2o.verify_cloud_size()

    def test_B_RF_iris2(self):
        parseResult = h2i.import_parse(bucket='smalldata', path='iris/iris2.csv', schema='put')
        h2o_cmd.runRFOnly(parseResult=parseResult, trees=6, timeoutSecs=10)

    def test_C_RF_poker100(self):
        parseResult = h2i.import_parse(bucket='smalldata', path='poker/poker100', schema='put')
        h2o_cmd.runRFOnly(parseResult=parseResult, trees=6, timeoutSecs=10)

    def test_D_GenParity1(self):
        parseResult = h2i.import_parse(bucket='smalldata', path='parity_128_4_100_quad.data', schema='put')
        h2o_cmd.runRFOnly(parseResult=parseResult, trees=50, timeoutSecs=15)

    def test_E_ParseManyCols(self):
        parseResult = h2i.import_parse(bucket='smalldata', path='fail1_100x11000.csv.gz', schema='put', timeoutSecs=10)
        inspect = h2o_cmd.runInspect(key=parseResult['destination_key'])

    def test_F_RF_covtype(self):
        parseResult = h2i.import_parse(bucket='datasets', path='UCI/UCI-large/covtype/covtype.data', schema='put', timeoutSecs=30)
        h2o_cmd.runRFOnly(parseResult=parseResult, trees=6, timeoutSecs=35, retryDelaySecs=0.5)

    def test_G_StoreView(self):
        h2i.delete_all_keys_at_all_nodes(timeoutSecs=30)

    def test_H_Slower_JUNIT(self):
        h2o.tear_down_cloud()
        h2o.build_cloud(node_count=2)
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
                'water.exec.RBigDataTest'
                ])
        # getting UDP receiver stack traces if we shut down quickly after Junit
        # may need to wait a little bit before shutdown?
        time.sleep(3)
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
