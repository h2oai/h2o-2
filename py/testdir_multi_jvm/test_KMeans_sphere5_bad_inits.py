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
    ### print [z,y,z]
    return [x,y,z]

def write_spheres_dataset(csvPathname, CLUSTERS, n):
    dsf = open(csvPathname, "w+")

    # going to do a bunch of spheres, with differing # of pts and R
    # R is radius of the spheres
    # separate them by 3 * the previous R
    # keep track of the centers so we compare to a sorted result from H2O
    centersList = []
    currentCenter = None
    totalRows = 0
    for sphereCnt in range(CLUSTERS):
        R = 10 * (sphereCnt+1)
        newOffset = [3*R,3*R,3*R]
        # figure out the next center
        if currentCenter is None:
            currentCenter = [0,0,0]
        else:
            currentCenter  = [a+b for a,b in zip(currentCenter, newOffset)] 
        centersList.append(currentCenter)

        # build a sphere at that center
        # pick a random # of points, from .5n to 1.5n
        numPts = random.randint(int(.5*n), int(1.5*n))
        print "currentCenter:", currentCenter, "R:", R, "numPts", numPts
        for i in range(numPts):
            xyz = get_xyz_sphere(R)
            xyzShifted  = [a+b for a,b in zip(xyz,currentCenter)] 
            dsf.write(",".join(map(str,xyzShifted))+"\n")
            totalRows += 1

    dsf.close()
    print "Spheres created:", len(centersList), "totalRows:", totalRows
    return centersList


class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = random.randint(0, sys.maxint)
        # for repeatability of case that fails
        SEED = 5987531387942634479

        random.seed(SEED)
        print "\nUsing random seed:", SEED

        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(1)
        else:
            h2o_hosts.build_cloud_with_hosts(1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_kmeans_sphere5(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        CLUSTERS = 5
        SPHERE_PTS = 10000
        csvFilename = 'syn_spheres100.csv'
        csvPathname = SYNDATASETS_DIR + '/' + csvFilename
        centersList = write_spheres_dataset(csvPathname, CLUSTERS, SPHERE_PTS)

        print "\nStarting", csvFilename
        parseKey = h2o_cmd.parseFile(csvPathname=csvPathname, key2=csvFilename + ".hex")

        # try 5 times, to see if all inits by h2o are good
        for trial in range(5):
            kwargs = {'k': CLUSTERS, 'epsilon': 1e-6, 'cols': None, 'destination_key': 'syn_spheres100.hex'}
            timeoutSecs = 30
            start = time.time()
            kmeans = h2o_cmd.runKMeansOnly(parseKey=parseKey, timeoutSecs=timeoutSecs, **kwargs)
            elapsed = time.time() - start
            print "kmeans end on ", csvPathname, 'took', elapsed, 'seconds.',\
                "%d pct. of timeout" % ((elapsed/timeoutSecs) * 100)

            kmeansResult = h2o_cmd.runInspect(key='syn_spheres100.hex')

            ### print h2o.dump_json(kmeans)
            ### print h2o.dump_json(kmeansResult)
            h2o_kmeans.simpleCheckKMeans(self, kmeans, **kwargs)

            # cluster centers can return in any order
            clusters = kmeansResult['KMeansModel']['clusters']
            clustersSorted = sorted(clusters, key=itemgetter(0))
            ### print clustersSorted

            print "\nh2o result, centers sorted"
            print clustersSorted
            print "\ngenerated centers"
            print centersList
            for i,center in enumerate(centersList):
                a = center
                b = clustersSorted[i]
                print "\nexpected:", a
                print "h2o:", b # h2o result
                aStr = ",".join(map(str,a))
                bStr = ",".join(map(str,b))
                iStr = str(i)
                self.assertAlmostEqual(a[0], b[0], delta=1, msg=aStr+"!="+bStr+". Sorted cluster center "+iStr+" x not correct.")
                self.assertAlmostEqual(a[1], b[1], delta=1, msg=aStr+"!="+bStr+". Sorted cluster center "+iStr+" y not correct.")
                self.assertAlmostEqual(a[2], b[2], delta=1, msg=aStr+"!="+bStr+". Sorted cluster center "+iStr+" z not correct.")

            print "Trial #", trial, "completed"




if __name__ == '__main__':
    h2o.unit_main()
