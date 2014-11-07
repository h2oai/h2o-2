import unittest, time, sys
# not needed, but in case you move it down to subdir
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_import as h2i
import h2o_browse as h2b

#
# This is a Python test for HEX-1198.
#
# Failing store view during listing.
#
# See https://0xdata.atlassian.net/browse/HEX-1198
#
class Basic(unittest.TestCase):

    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    # Test store view listing
    def test_A_store_view(self):
        # size of H2O store
        store_size = 0
        # import data to have more files in the system
        r = h2i.import_only(bucket='smalldata', path='iris/*')
        store_size += len(r[0]['files'])
        r = h2i.import_only(bucket='smalldata', path='covtype/*')
        store_size += len(r[0]['files'])

        # list all items
        r = h2o.nodes[0].store_view(view=store_size)
        self.assertEqual(store_size, len(r['keys']))

        # list over views including only 3 items
        items_per_page = 3                  # items per page
        pages = (store_size / items_per_page)    # number of pages
        if (store_size % items_per_page != 0): pages += 1
        offset = 0 # running offset
        cnt_items = 0  # counter of returned items
        for p in range(0,pages):
            r = h2o.nodes[0].store_view(offset=offset, view=items_per_page)
            print h2o.dump_json(r)
            cnt_items += len(r['keys']) 
            offset += items_per_page

        self.assertEqual(store_size, cnt_items)

if __name__ == '__main__':
    h2o.unit_main()
