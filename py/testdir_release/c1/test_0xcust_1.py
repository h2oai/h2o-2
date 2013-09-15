import unittest, time, sys
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_import as h2i

print "You need the special config json, and the private key, that lets you run h2o as <the user>"
print "You want the private key to avoid putting a password into cleartext"
print "You might also need some special mounts setup on the target machines to point to the ..."
print "those will be schema=local resolutions with import_parse()"
class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(node_count=1)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_Basic(self):
        for n in h2o.nodes:
            c = n.get_cloud()
            self.assertEqual(c['cloud_size'], len(h2o.nodes), 'inconsistent cloud size')

    def test_RF_poker100(self):
        trees = 6
        timeoutSecs = 20
        csvPathname = 'standard/covtype.data'
        parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, schema='local')
        h2o_cmd.runRF(parseResult=parseResult, trees=trees, timeoutSecs=timeoutSecs)

if __name__ == '__main__':
    h2o.unit_main()
