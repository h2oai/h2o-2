import os, json, unittest, time, shutil, sys, getpass
import h2o

class PetaTest(unittest.TestCase):

    def testAll(self):
        try:
            h2o.build_cloud(node_count=2)

            # we don't have the port or ip configuration here
            # that util/h2o.py does? Keep this in synch with spawn_h2o there.
            # also don't have --nosigar here?
            (ps, stdout, stderr) = h2o.spawn_cmd('junit', [
                    'java',
                    '-Dh2o.arg.ice_root='+h2o.tmp_dir('ice.'),
                    '-Dh2o.arg.name=pytest-'+getpass.getuser(),
                    '-Dh2o.arg.ip='+h2o.get_ip_address(),
                    '-ea', '-jar', h2o.find_file('build/h2o.jar'),
                    '-mainClass', 'org.junit.runner.JUnitCore',
                    # The tests
                    'test.ParserTest',
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
