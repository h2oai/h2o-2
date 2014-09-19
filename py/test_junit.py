import unittest, time, sys
import h2o

class TestJUnit(unittest.TestCase):

    def test_A_all_junit(self):
        try:
            h2o.build_cloud(node_count=2, java_heap_GB=2)

            # we don't have the port or ip configuration here
            # that util/h2o.py does? Keep this in synch with spawn_h2o there.
            # also don't have --nosigar here?
            (ps, stdout, stderr) = h2o.spawn_cmd('junit', [
                    'java',
                    '-Xms2G',
                    '-Xmx2G',
                    '-Dh2o.arg.ice_root='+h2o.tmp_dir('ice.'),
                    '-Dh2o.arg.name='+h2o.cloud_name(),
                    '-Dh2o.arg.ip='+h2o.get_ip_address(),
                    '-Dh2o.arg.port=54666',
                    '-ea', '-jar', h2o.find_file('target/h2o.jar'),
                    '-mainClass', 'org.junit.runner.JUnitCore',
                    # The all test suite
                    'water.suites.AllTestsSuite'
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
    
if __name__ == '__main__':
    h2o.unit_main()
