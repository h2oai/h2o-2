import os, json, unittest, time, shutil, sys
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_kmeans, h2o_hosts
from operator import itemgetter
import random
import math

# a truly uniform sphere
# http://stackoverflow.com/questions/5408276/python-uniform-spherical-distribution
# While the author prefers the discarding method for spheres, for completeness 
# he offers the exact solution: http://stackoverflow.com/questions/918736/random-number-generator-that-produces-a-power-law-distribution/918782#918782
# In spherical coordinates, taking advantage of the sampling rule:
# http://stackoverflow.com/questions/2106503/pseudorandom-number-generator-exponential-distribution/2106568#2106568
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
    return (x,y,z)

def write_syn_dataset(csvPathname, n, SEED):
    print "Make 3 spheres, well separated, with different 1x, 2x, 3x #of entries"
    print "6M rows total"
    dsf = open(csvPathname, "w+")

    # radius of the spheres
    R1 = 10
    R2 = 20
    R3 = 30

    print "Centers of synthetic:"
    A = (x1,y1,z1) = (100,100,100)
    print A
    B = (x2,y2,z2) = (200,200,200)
    print B
    C = (x3,y3,z3) = (300,300,300)
    print C

    for i in range(n):
        (x,y,z) = get_xyz_sphere(R1)
        dsf.write("%s %s %s\n" % (x+x1, y+y1, z+z1))

        # 2x as many of the 2nd
        (x,y,z) = get_xyz_sphere(R2)
        dsf.write("%s %s %s\n" % (x+x2, y+y2, z+z2))
        (x,y,z) = get_xyz_sphere(R2)
        dsf.write("%s %s %s\n" % (x+x2, y+y2, z+z2))

        # 3x as many of the 3rd
        (x,y,z) = get_xyz_sphere(R3)
        dsf.write("%s %s %s\n" % (x+x3, y+y3, z+z3))
        (x,y,z) = get_xyz_sphere(R3)
        dsf.write("%s %s %s\n" % (x+x3, y+y3, z+z3))
        (x,y,z) = get_xyz_sphere(R3)
        dsf.write("%s %s %s\n" % (x+x3, y+y3, z+z3))

    dsf.close()
    return


def show_results(csvPathname, parseKey, model_key, centers, destination_key):
    kmeansApplyResult = h2o.nodes[0].kmeans_apply(
        data_key=parseKey['destination_key'], model_key=model_key,
        destination_key=destination_key)
    # print h2o.dump_json(kmeansApplyResult)
    inspect = h2o_cmd.runInspect(None, destination_key)
    h2o_cmd.infoFromInspect(inspect, csvPathname)

    kmeansScoreResult = h2o.nodes[0].kmeans_score(
        key=parseKey['destination_key'], model_key=model_key)
    score = kmeansScoreResult['score']
    rows_per_cluster = score['rows_per_cluster']
    sqr_error_per_cluster = score['sqr_error_per_cluster']

    for i,c in enumerate(centers):
        print "\ncenters["+str(i)+"]: ", centers[i]
        print "rows_per_cluster["+str(i)+"]: ", rows_per_cluster[i]
        print "sqr_error_per_cluster["+str(i)+"]: ", sqr_error_per_cluster[i]


class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = random.randint(0, sys.maxint)
        # SEED = 
        random.seed(SEED)
        print "\nUsing random seed:", SEED

        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(3)
        else:
            h2o_hosts.build_cloud_with_hosts(3)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_kmeans_sphere3(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        csvFilename = 'syn_spheres3_' + str(SEED) + '.csv'
        csvPathname = SYNDATASETS_DIR + '/' + csvFilename
        write_syn_dataset(csvPathname, 1000000, SEED)

        print "\nStarting", csvFilename
        parseKey = h2o_cmd.parseFile(csvPathname=csvPathname, key2=csvFilename + ".hex")

        kwargs = {'k': 3, 'epsilon': 1e-6, 'cols': None, 'destination_key': 'spheres3.hex'}
        timeoutSecs = 30
        start = time.time()
        kmeans = h2o_cmd.runKMeansOnly(parseKey=parseKey, timeoutSecs=timeoutSecs, **kwargs)
        elapsed = time.time() - start
        print "kmeans end on ", csvPathname, 'took', elapsed, 'seconds.', "%d pct. of timeout" % ((elapsed/timeoutSecs) * 100)

        centers = h2o_kmeans.bigCheckResults(self, kmeans, csvPathname, parseKey, 'd', **kwargs)
        # cluster centers can return in any order
        centersSorted = sorted(centers, key=itemgetter(0))

        self.assertAlmostEqual(centersSorted[0][0],100,delta=.2)
        self.assertAlmostEqual(centersSorted[1][0],200,delta=.2)
        self.assertAlmostEqual(centersSorted[2][0],300,delta=.2)

        self.assertAlmostEqual(centersSorted[0][1],100,delta=.2)
        self.assertAlmostEqual(centersSorted[1][1],200,delta=.2)
        self.assertAlmostEqual(centersSorted[2][1],300,delta=.2)

        self.assertAlmostEqual(centersSorted[0][2],100,delta=.2)
        self.assertAlmostEqual(centersSorted[1][2],200,delta=.2)
        self.assertAlmostEqual(centersSorted[2][2],300,delta=.2)

        show_results(csvPathname, parseKey, model_key, centers, 'd')

if __name__ == '__main__':
    h2o.unit_main()
