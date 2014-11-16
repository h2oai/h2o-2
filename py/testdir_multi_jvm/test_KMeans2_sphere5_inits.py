import unittest, time, sys, random, math
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_kmeans, h2o_import as h2i
from operator import itemgetter
# want named tuples
import collections

# a truly uniform sphere
# http://stackoverflow.com/questions/5408276/python-uniform-spherical-distribution
# While the author prefers the discarding method for spheres, for completeness 
# he offers the exact solution: http://stackoverflow.com/questions/918736/random-number-generator-that-produces-a-power-law-distribution/918782#918782
# In spherical coordinates, taking advantage of the sampling rule:
# http://stackoverflow.com/questions/2106503/pseudorandom-number-generator-exponential-distribution/2106568#2106568

# let h2o randomize seed
BAD_SEED = None
MAX_ITER = 500
TRIALS = 4
# INIT='Furthest'
INIT='PlusPlus'
# INIT=''
TEST_CASE = 1
# BAD_SEED = 5010213207974401134

def get_xyz_sphere(R):
    phi = random.uniform(0, 2 * math.pi)
    costheta = random.uniform(-1,1)
    u = random.random() # 0 to 1
    theta = math.acos(costheta)
    r = R * (u ** (1.0/3))

    # now you have a (r, theta, phi) group which can be transformed to (x, y, z) 
    # in the usual way
    x = r * math.sin(theta) * math.cos(phi)
    y = r * math.sin(theta) * math.sin(phi)
    z =  r * math.cos(theta) 
    ### print [z,y,z]
    return [x,y,z]

def write_spheres_dataset(csvPathname, CLUSTERS, n):
    dsf = open(csvPathname, "w+")

    # going to do a bunch of spheres, with differing # of pts and R
    # R is radius of the spheres
    # separate them by 3 * the previous R
    # keep track of the centers so we compare to a sorted result from H2O
    expectedCenters = []
    currentCenter = None
    totalRows = 0
    print ""
    for sphereCnt in range(CLUSTERS):

        if TEST_CASE==1:
            R = 0.0
            newOffset = [0.0, 0.0, 10.0]
        else:
            R = 10 * (sphereCnt+1)
            newOffset = [3*R, 3*R, 3*R]

        # figure out the next center
        if currentCenter is None:
            currentCenter = [0.0, 0.0, 0.0]
        else:
            currentCenter  = [a+b for a,b in zip(currentCenter, newOffset)] 
        expectedCenters.append(currentCenter)

        # build a sphere at that center
        if TEST_CASE==1:
            numPts = 10
        else:
            # pick a random # of points, from .5n to 1.5n
            numPts = random.randint(int(.5*n), int(1.5*n))

        print "currentCenter:", currentCenter, "R:", R, "numPts", numPts
        for i in range(numPts):
            if TEST_CASE==1:
                xyzShifted = (0.0, 0.0, sphereCnt*10.0)
            else:
                xyz = get_xyz_sphere(R)
                xyzShifted  = [a+b for a,b in zip(xyz,currentCenter)] 

            dsf.write(",".join(map(str,xyzShifted))+"\n")
            totalRows += 1

    dsf.close()
    print "Spheres created:", len(expectedCenters), "totalRows:", totalRows
    return expectedCenters


class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        # use the known bad seed if it's set. otherwise should be None
        SEED = h2o.setup_random_seed(seed=BAD_SEED)
        h2o.init(3)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_KMeans2_sphere5_inits(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        CLUSTERS = 5
        SPHERE_PTS = 10000
        csvFilename = 'syn_spheres100.csv'
        csvPathname = SYNDATASETS_DIR + '/' + csvFilename
        expectedCenters = write_spheres_dataset(csvPathname, CLUSTERS, SPHERE_PTS)

        print "\nStarting", csvFilename
        parseResult = h2i.import_parse(path=csvPathname, schema='put', hex_key=csvFilename + ".hex")

        # try 5 times, to see if all inits by h2o are good
        savedResults = []
        Result = collections.namedtuple('Result', 
            'trial clusters size cluster_variances error iterations normalized max_iter clustersSorted')

        # save the best for comparison. Print messages when we update best
        sameAsBest = 0
        # big number? to init
        bestResult = Result(None, None, None, None, None, None, None, None, None)
        for trial in range(TRIALS):
            # pass SEED so it's repeatable
            kwargs = {
                'normalize': 0,
                'k': CLUSTERS, 
                'max_iter': MAX_ITER, 
                'initialization': INIT,
                # 'initialization': 'PlusPlus',
                'destination_key': 'syn_spheres100.hex', 
                'seed': SEED
            }

            timeoutSecs = 30
            start = time.time()
            kmeansResult = h2o_cmd.runKMeans(parseResult=parseResult, timeoutSecs=timeoutSecs, **kwargs)
            elapsed = time.time() - start
            print "kmeans end on ", csvPathname, 'took', elapsed, 'seconds.',\
                "%d pct. of timeout" % ((elapsed/timeoutSecs) * 100)

            # see if we took the full limit to get an answer
            
            # inspect of model doesn't work
            # kmeansResult = h2o_cmd.runInspect(key='syn_spheres100.hex')
            ### print h2o.dump_json(kmeans)
            ### print h2o.dump_json(kmeansResult)
            h2o_kmeans.simpleCheckKMeans(self, kmeansResult, **kwargs)

            model = kmeansResult['model']
            clusters = model["centers"]
            size = model["size"]
            cluster_variances = model["within_cluster_variances"]
            # round to int to avoid fp error when saying "same"
            error = int(model["total_within_SS"])
            iterations = model["iterations"]
            normalized = model["normalized"]
            max_iter = model["max_iter"]
            # clustersSorted = sorted(clusters, key=itemgetter(2))
            clustersSorted = sorted(clusters)

            r = Result (
                trial,
                clusters,
                size,
                cluster_variances,
                error,
                iterations,
                normalized,
                max_iter,
                clustersSorted,
            )

            savedResults.append(r)

            if iterations >= (max_iter-1): # h2o hits the limit at max_iter-1..shouldn't hit it
                raise Exception("KMeans unexpectedly took %s iterations..which was the full amount allowed by max_iter %s", 
                    (iterations, max_iter))

            print "iterations", iterations
            ### print clustersSorted

            # For now, just analyze the one with the lowest error
            # we could analyze how many are not best, and how many are best (maybe just look at error
            print "savedResults, error"
            print r.error
            if bestResult.error and r.error <= bestResult.error:
                sameAsBest += 1
                # we can check that if it has the same error, the sizes should be the same (integer) and reflects centers?
                # should 
                if r.size!=bestResult.size:
                    raise Exception("Would expect that if two trials got the same error (rounded to int), the cluster sizes would likely be the same? %s %s" % 
                        (r.size, bestResult.size))

            if not bestResult.error: # init case
                bestResult = r 
            elif r.error < bestResult.error:
                print "Trial", r.trial, "has a lower error", r.error, "than current lowest error", bestResult.error
                print "Using it for best now"
                bestResult = r

            print "Trial #", trial, "completed"
                
        print "\nApparently, %s out of %s trials, got the same best error: %s  (lowest) " % (sameAsBest, TRIALS, bestResult.error)
        print "\nh2o best result was from trial %s, centers sorted:" % bestResult.trial
        print bestResult.clustersSorted
        print "\ngenerated centers for comparison"
        print expectedCenters
        for i,center in enumerate(expectedCenters):
            a = center
            bb = bestResult.clustersSorted
            print "bb:", bb
            b = bb[i]
            print "\nexpected:", a
            print "h2o:", b # h2o result
            aStr = ",".join(map(str,a))
            bStr = ",".join(map(str,b))
            iStr = str(i)
            self.assertAlmostEqual(a[0], b[0], delta=1, msg=aStr+"!="+bStr+". Sorted cluster center "+iStr+"; x not correct.")
            self.assertAlmostEqual(a[1], b[1], delta=1, msg=aStr+"!="+bStr+". Sorted cluster center "+iStr+"; y not correct.")
            self.assertAlmostEqual(a[2], b[2], delta=1, msg=aStr+"!="+bStr+". Sorted cluster center "+iStr+"; z not correct.")

            # fix: should check size too. Really should format expected into the tuple that the h2o_kmeans checker uses
            # the c5 testdir_release stuff has a checker..for centers, size, error?

if __name__ == '__main__':
    h2o.unit_main()
