import unittest, time, sys, random, math
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_kmeans, h2o_import as h2i

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

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init(3)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_kmeans_sphere3_fvec(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        csvFilename = 'syn_spheres3_' + str(SEED) + '.csv'
        csvPathname = SYNDATASETS_DIR + '/' + csvFilename
        write_syn_dataset(csvPathname, 1000000, SEED)

        print "\nStarting", csvFilename
        hex_key = csvFilename + ".hex"
        parseResult = h2i.import_parse(path=csvPathname, schema='put', hex_key=hex_key)

        for trial in range(10):
            # reuse the same seed, to get deterministic results (otherwise sometimes fails
            kwargs = {
                'k': 3, 
                'max_iter': 25,
                'initialization': 'Furthest',
                'destination_key': 'spheres3.hex', 
                # 'seed': 265211114317615310,
                'seed': 0,
                }

            timeoutSecs = 90
            start = time.time()
            kmeans = h2o_cmd.runKMeans(parseResult=parseResult, timeoutSecs=timeoutSecs, **kwargs)
            elapsed = time.time() - start
            print "kmeans end on ", csvPathname, 'took', elapsed, 'seconds.', "%d pct. of timeout" % ((elapsed/timeoutSecs) * 100)

            (centers, tupleResultList)  = h2o_kmeans.bigCheckResults(self, kmeans, csvPathname, parseResult, 'd', **kwargs)

            expected = [
                ([100, 100, 100], 1000000,   60028168),
                ([200, 200, 200], 2000000,  479913618),
                ([300, 300, 300], 3000000, 1619244994),
            ]
            # all are multipliers of expected tuple value
            allowedDelta = (0.01, 0.01, 0.01) 
            h2o_kmeans.compareResultsToExpected(self, tupleResultList, expected, allowedDelta, trial=trial)

            #too slow
            #gs = h2o.nodes[0].gap_statistic(source=hex_key, k_max=5, timeoutSecs=3000)
            #print "gap_statistic:", h2o.dump_json(gs)


if __name__ == '__main__':
    h2o.unit_main()
