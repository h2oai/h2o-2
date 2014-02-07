import unittest, time, sys
import h2o

class TestJUnit(unittest.TestCase):

    def test_A_fast_junit(self):
        try:
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
                    #'hex.GLMGridTest',
                    'hex.HistogramTest',
                    'hex.GLMTest',
                    'hex.KMeansTest',
                    'hex.MinorityClassTest',
                    'hex.NeuralNetSpiralsTest',
                    'hex.rf.RandomForestTest',
                    'hex.rf.RFPredDomainTest',
                    'water.AtomicTest',
                    'water.AutoSerialTest',
                    'water.BitCmpTest',
                    #'water.ConcurrentKeyTest.java',
                    'water.KeyToString',
                    'water.KVTest',
                    #'water.KVSpeedTest',
                    'water.api.RStringTest',
                    'water.parser.DatasetCornerCasesTest',
                    'water.parser.ParseCompressedAndXLSTest',
                    'water.parser.ParseFolderTest',
                    'water.parser.ParseProgressTest',
                    'water.parser.ParserTest',
                    'water.parser.RReaderTest',
                    'water.score.ScorePmmlTest',
                    'water.score.ScoreTest'
                    ])

            rc = ps.wait(None)
            out = file(stdout).read()
            err = file(stderr).read()
            if rc is None:
                ps.terminate()
                raise Exception("junit timed out.\nstdout:\n%s\n\nstderr:\n%s" % (out, err))
            elif rc != 0:
                raise Exception("junit failed.\nstdout:\n%s\n\nstderr:\n%s" % (out, err))

        finally:
            h2o.tear_down_cloud()

    def test_B_slow_junit(self):
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
                'hex.MinorityClassTest'
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
