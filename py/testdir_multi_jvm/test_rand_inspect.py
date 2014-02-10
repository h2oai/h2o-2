import unittest, random, sys, time
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_util, h2o_import as h2i, h2o_browse as h2b
import h2o_print as h2p

def ch1(n):
    return 0
def ch2(n):
    return n
def ch3(n):
    return n
def ch4(n):
    return int(n/2)
def ch5(n):
    return random.randint(0,n-1)

def good_choices(n):
    ch = h2o_util.choice_with_probability([(ch1,0.1), (ch2,0.1), (ch3,0.1), (ch4,0.1), (ch5,0.6)])
    return ch(n)

INVISIBLE_AUTOFRAME = True
class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED, localhost
        SEED = h2o.setup_random_seed()
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(3)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()


    def test_rand_inspect(self):
        ### h2b.browseTheCloud()

        ### h2b.browseTheCloud()
        csvFilename = 'covtype.data'
        csvPathname = 'standard/'+ csvFilename
        hex_key = csvFilename + ".hex"
        print "\n" + csvPathname

        parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, schema='put', hex_key=hex_key, timeoutSecs=10)
        destination_key = parseResult['destination_key']
        print csvFilename, 'parse time:', parseResult['response']['time']
        print "Parse result['destination_key']:", destination_key 

        def inspect_and_check(nodeX, destination_key, offset, view, inspectOld=None):
            inspectNew = h2o_cmd.runInspect(h2o.nodes[nodeX], destination_key, offset=offset, view=view)
            if h2o.beta_features:
                pass
                # print "Inspect2:", h2o.dump_json(inspectNew)
            else:
                pass
                # print "Inspect:", h2o.dump_json(inspectNew)

            # FIX! get min/max/mean/variance for a col too?
            constantNames = [
                ('num_cols', 'numCols'),
                ('num_rows', 'numRows'),
                ('value_size_bytes', 'byteSize'),
                ('cols', 'cols'),
                ]

            colNames = [
                ('num_missing_values', 'naCnt'),
                ]

            for (i,j) in constantNames:
                # check the fields, even if you don't have a previous one to compare to
                if h2o.beta_features:
                    # hack in extra info for now, from the new names to old names
                    if not j in inspectNew:
                        raise Exception("Can't find %s, Inspect2 result should have it?" % j)
                    inspectNew[i] = inspectNew[j]

                # don't compare if cols
                if inspectOld  and i != 'cols':
                    if h2o.beta_features and i=='value_size_bytes': # Inspect2 should be smaller
                        self.assertGreater(inspectOld[i], inspectNew[i])
                        
                    else:
                        # for cols it will just compare length?
                        self.assertEqual(inspectOld[i], inspectNew[i])

                if i=='cols':
                    for (m,n) in colNames:
                        if h2o.beta_features:
                            if not n in inspectNew[i][0]:
                                print h2o.dump_json(inspectNew[i][0])
                                raise Exception("Can't find %s, Inspect2 result['cols'][0] should have it?" % n)
                            inspectNew[i][0][m] = inspectNew[i][0][n]
                        # just compare 0
                        if inspectOld is not None:
                            self.assertEqual(inspectOld[i][0][m], inspectNew[i][0][m])

            return inspectNew

        # going to use this to compare against future. num_rows/num_cols should always
        # be the same, regardless of the view. just a coarse sanity check
        origInspect = inspect_and_check(0, destination_key, 0, 1, None)
        h2o.verboseprint(h2o.dump_json(origInspect))
        origStoreViewResult = h2o_cmd.runStoreView(offset=0, view=1024, timeoutSecs=60)

        num_rows = origInspect['num_rows']
        num_cols = origInspect['num_cols']

        lenNodes = len(h2o.nodes)
        for trial in range (10):
            h2p.green_print("\nTrial", trial)
            # we want to use the boundary conditions, so have two level of random choices
            offset = good_choices(num_rows)
            view = good_choices(num_cols)
            # randomize the node used
            nodeX = random.randint(0,lenNodes-1)
            print "nodeX:", nodeX, "offset:", offset, "view:", view
            h2o.beta_features = False
            inspect_and_check(nodeX,destination_key,offset,view,origInspect)
            print "trying Inspect2 by flipping h2o.nodes[0].beta_features"
            h2o.beta_features = True
            # delay between the two inspects...bug around not getting autoframe in storeview?
            time.sleep(1)            
            inspect_and_check(nodeX,destination_key,offset,view,origInspect)
            h2o.beta_features = False

            # a fvec frame should have been created in the storeView
            time.sleep(1)            

            # loop looking for the autoframe to show up
            # o = len(origStoreViewResult['keys'])
            o = h2i.count_keys_at_all_nodes()
            retry = 0
            okay = False
            while retry==0 or not okay:
                newStoreViewResult = h2o_cmd.runStoreView(offset=0, view=1024, timeoutSecs=60)
                ## p = len(newStoreViewResult['keys'])
                p = h2i.count_keys_at_all_nodes()
                print "number of keys in the two StoreViews, o:", o, "p:", p
                ## print "newStoreViewResult:", h2o.dump_json(newStoreViewResult)
                oOkay = {1, 2, 3, 4, 5, 6, 7, 8}
                pOkay = {1, 2, 3, 4, 5}
                print o, pOkay, p, oOkay
                if (o in oOkay) and (p in pOkay):
                    print "Good"
                    okay = True
                else:
                    print "Unexpected o,p after autoframe, looking at total keys in system: %s %s" % (o,p)

                if retry==10:
                    raise Exception("StoreView didn't get autoframe, after %s retries" % retry)
                ## h2b.browseJsonHistoryAsUrlLastMatch("StoreView")

                # so he gets recreated??
                deleted = h2i.delete_keys_at_all_nodes(pattern='autoframe')
                # The autoframe key may not show up!!
                if INVISIBLE_AUTOFRAME:
                    # can be 1 or 2
                    if not(deleted==0 or deleted==1):
                        msg = "Should have deleted a total of 0 or 1 keys, looking at all nodes. Did %s" % deleted
                        raise Exception(msg)
                else:
                    # can be 1 or 2
                    if not(deleted==1):
                        msg = "Should have deleted a total of 1 keys, looking at all nodes. Did %s" % deleted
                
                time.sleep(1)
                retry += 1


if __name__ == '__main__':
    h2o.unit_main()
