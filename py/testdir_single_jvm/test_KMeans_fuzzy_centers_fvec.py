import unittest, random, sys, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i
import h2o_kmeans
import h2o_exec as h2e

DO_TWO_CLUSTER = True

def calc_best_distance(centers, dataset):
    # dataset is a list of lists (values)
    # centers is the result KMeans centers. 
    # assume they are close to the known generated centers
    totalError = 0
    clusterMembership = [0 for i in centers]
    clusterVariance = [0 for i in centers]
    for data in dataset:
        smallestError = None
        # we could keep track of i, so we know the number of entries in each cluster
        for i,center in enumerate(centers):
            error = sum([(c - d) ** 2 for c,d in zip(center, data)]) 
            if not smallestError or (error < smallestError):
                smallestError = error
                smallestI = i
        # now that we know where the smallest error is, add it to 
        totalError += smallestError
        clusterMembership[smallestI] += 1
        clusterVariance[smallestI] += smallestError

    print "clusterMembership:", clusterMembership
    print "clusterVariance:", clusterVariance
    return totalError

def write_syn_dataset(csvPathname, rowCount, centers, SEED):

    r1 = random.Random(SEED)
    dsf = open(csvPathname, "w+")

    # add small deltas around the centers given
    # FIX! should I shuffle the dataset afterwards?
    dataset = []
    for center in centers:
        for i in range(rowCount):
            rowData = []
            for c in center:
                r = random.randint(-1,1)
                # doesn't fail
                # r = 0
                rowData.append(c + r)

            rowDataCsv = ",".join(map(str,rowData))
            dsf.write(rowDataCsv + "\n")
            dataset.append(rowData)

    dsf.close()
    return dataset


class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init(1,java_heap_GB=4)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_KMeans_fuzzy_centers_fvec(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()

        if DO_TWO_CLUSTER:
            genCenters = [
                [100, 100, 100, 100, 100, 100],
                [200, 200, 200, 200, 200, 200],
            ]

            genCenters = [
                [100, 100],
                [200, 200],
            ]

        else:
            genCenters = [
                [100, 100, 100, 100, 100, 100],
                [110, 110, 110, 110, 110, 110],
                [120, 120, 120, 120, 120, 120],
                [130, 130, 130, 130, 130, 130],
                ]
    
        ### h2b.browseTheCloud()
        lenNodes = len(h2o.nodes)

        rowCount = 10000
        expected = [(g, rowCount, None) for g in genCenters]
        allowedDelta = (0.2, 0.2, 0.2, 0.2, 0.2, 0.2)
        allowedDelta = (0.2, 0.2)
        worstError = None
        bestError = None

        timeoutSecs = 60
        hex_key = 'cA'

        print "Generate synthetic dataset with first column constant = 0 and see what KMeans does"
        csvFilename = 'syn_' + str(SEED) + "_" + str(rowCount) + '.csv'
        csvPathname = SYNDATASETS_DIR + '/' + csvFilename

        print "Creating random", csvPathname
        dataset = write_syn_dataset(csvPathname, rowCount, genCenters, SEED)
        parseResult = h2i.import_parse(path=csvPathname, schema='put', hex_key=csvFilename + ".hex")
        print "Parse result['destination_key']:", parseResult['destination_key']

        allErrors = []
        for trial in range(10):
            seed = random.randint(0, sys.maxint)
            kwargs = {
                'seed': seed,
                'k': len(genCenters), 
                'initialization': 'PlusPlus', 
                'destination_key': 'k.hex', 
                'max_iter': 1000 }
            kmeans = h2o_cmd.runKMeans(parseResult=parseResult, timeoutSecs=60, **kwargs)
            (centers, tupleResultList) = h2o_kmeans.bigCheckResults(self, kmeans, csvPathname, parseResult, 'd', **kwargs)
            # save the predicted
            h2o.nodes[0].csv_download(src_key='d', csvPathname='kmeans_predict.csv')

            # check center list (first center) has same number of cols as source data
            self.assertEqual(len(genCenters[0]), len(centers[0]),
                "kmeans first center doesn't have same # of values as dataset row %s %s" % (len(genCenters[0]), len(centers[0])))
            h2o_kmeans.compareResultsToExpected(self, tupleResultList, expected, allowedDelta, trial=trial)

            error = kmeans['model']['total_within_SS']
            within_cluster_variances = kmeans['model']['within_cluster_variances']
            print "trial:", trial, "within_cluster_variances:", within_cluster_variances

            # compute the sum of the squares of the distance for each cluster
            # for each row, we 
            # returns a tuple of numers for each center
            genDistances = calc_best_distance(centers, dataset)
            print "trial:", trial, "genDistances:", genDistances
            print "trial:", trial, "centers:", centers
            print "trial:", trial, "error:", error
            if (abs(genDistances - error)) > (.001 * genDistances):
                raise Exception("genDistances: %s error: %s are too different" % (genDistances, error))
    
            if not bestError or error < bestError:
                print 'Found smaller error:', error
                bestError = error
                bestCenters = centers
                bestSeed = seed
                bestTrial = trial

            if not worstError or error > worstError:
                print 'Found larger error:', error
                worstError = error

            allErrors.append(error)

        print "bestTrial:", bestTrial
        print "bestError:", bestError
        print "worstError:", worstError
        print "bestCenters:", bestCenters
        print "bestSeed:", bestSeed
        print "allErrors:", allErrors


if __name__ == '__main__':
    h2o.unit_main()
