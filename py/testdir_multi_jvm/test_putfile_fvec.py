import unittest, time, sys
sys.path.extend(['.','..','../..','py'])


import h2o, h2o_util
import itertools

def file_to_put():
#TODO handle command line options to allow put arbitratry file
    return h2o.find_file('smalldata/poker/poker-hand-testing.data')

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init(3)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()
        #pass

    def test_A_putfile(self):
        cvsfile    = file_to_put()
        node       = h2o.nodes[0]
        key        = node.put_file(cvsfile)
        resultSize = node.inspect(key)['byteSize']
        origSize   = h2o_util.get_file_size(cvsfile)
        self.assertEqual(origSize,resultSize)
    
    def test_B_putfile_to_all_nodes(self):

        cvsfile  = file_to_put()
        origSize = h2o_util.get_file_size(cvsfile)
        for node in h2o.nodes:
            key        = node.put_file(cvsfile)
            resultSize = node.inspect(key)['byteSize']
            self.assertEqual(origSize,resultSize)
    

if __name__ == '__main__':
    h2o.unit_main()
