import os, json, unittest, time, shutil, sys
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts
import itertools

def file_to_put():
#TODO handle command line options to allow put arbitratry file
    return h2o.find_file('smalldata/poker/poker-hand-testing.data')

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(3)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()
        #pass

    def test_A_putfile(self):
        cvsfile    = h2o.find_file(file_to_put())
        node       = h2o.nodes[0]
        key        = node.put_file(cvsfile)
        resultSize = node.inspect(key)['value_size_bytes']
        origSize   = h2o.get_file_size(cvsfile)
        self.assertEqual(origSize,resultSize)
    
    def test_B_putfile_to_all_nodes(self):

        cvsfile  = h2o.find_file(file_to_put())
        origSize = h2o.get_file_size(cvsfile)
        for node in h2o.nodes:
            key        = node.put_file(cvsfile)
            resultSize = node.inspect(key)['value_size_bytes']
            self.assertEqual(origSize,resultSize)
    
    def test_C_putfile_and_getfile(self):

        cvsfile = h2o.find_file(file_to_put())
        node    = h2o.nodes[0]
        key     = node.put_file(cvsfile)
        r       = node.get_key(key)
        f       = open(cvsfile)
        self.diff(r, f)
        f.close()

    def test_D_putfile_and_getfile_to_all_nodes(self):

        cvsfile = h2o.find_file(file_to_put())
        for node in h2o.nodes:
            key    = node.put_file(cvsfile)
            r      = node.get_key(key)
            f      = open(cvsfile)
            self.diff(r, f)
            f.close()

    def diff(self,r, f):
        for (r_chunk,f_chunk) in itertools.izip(r.iter_content(1024), h2o.iter_chunked_file(f, 1024)):
            self.assertEqual(r_chunk,f_chunk)

if __name__ == '__main__':
    h2o.unit_main()
