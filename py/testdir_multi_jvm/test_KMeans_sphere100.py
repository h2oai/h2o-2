import os, json, unittest, time, shutil, sys
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_kmeans, h2o_hosts
import random
import math

# a truly uniform sphere
# http://stackoverflow.com/questions/5408276/python-uniform-spherical-distribution
# While the author prefers the discarding method for spheres, for completeness 
# he offers the exact solution: http://stackoverflow.com/questions/918736/random-number-generator-that-produces-a-power-law-distribution/918782#918782
# In spherical coordinates, taking advantage of the sampling rule:
# http://stackoverflow.com/questions/2106503/pseudorandom-number-generator-exponential-distribution/2106568#2106568
global CLUSTERS, SPHERE_PTS
CLUSTERS = 3
SPHERE_PTS = 10000
ALLOWED_DELTA = 1

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
    print "To keep life interesting:"
    print "make the multiplier, between 3 and 9 in just one direction"
    print "pick x, y, or z direction randomly"
    print "We tack the centers created, and assert against the H2O results, so 'correct' is checked"
    print "Increasing radius for each basketball. (R)"
    centersList = []
    currentCenter = None
    totalRows = 0
    for sphereCnt in range(CLUSTERS):
        R = 10 * (sphereCnt+1)
        if (1==0):
            jump = random.randint(3*R,(3*R)+9)
            xyzChoice = random.randint(0,2)
        else:
            print "FIX! temporarily limiting the center jumps to z dimension for easy understanding"
            jump = 3*R
            xyzChoice = 2

        newOffset = [0,0,0]
        newOffset[xyzChoice] = jump

        # figure out the next center
        if currentCenter is None:
            currentCenter = [0,0,0]
        else:
            lastCenter = currentCenter
            currentCenter  = [a+b for a,b in zip(currentCenter, newOffset)] 
            if (sum(currentCenter) - sum(lastCenter) < (len(currentCenter)* ALLOWED_DELTA)):
                print "ERROR: adjacent centers are too close for our sort algorithm"
                print "currentCenter:", currentCenter, "lastCenter:", lastCenter
                raise Exception
                
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
        # SEED = 5987531387942634479
        SEED = 6050079225893213627
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

    def test_kmeans_sphere100(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        csvFilename = 'syn_spheres100.csv'
        csvPathname = SYNDATASETS_DIR + '/' + csvFilename
        centersList = write_spheres_dataset(csvPathname, CLUSTERS, SPHERE_PTS)

        print "\nStarting", csvFilename
        parseKey = h2o_cmd.parseFile(csvPathname=csvPathname, key2=csvFilename + ".hex")

        # try 5 times, to see if all inits by h2o are good
        for trial in range(3):
            kwargs = {'k': CLUSTERS, 'epsilon': 1e-6, 'cols': None, 'destination_key': 'syn_spheres100.hex'}
            timeoutSecs = 100
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
            # the way we create the centers above, if we sort on the sum of xyz
            # we should get the order the same as when they were created.
            # to be safe, we'll sort the centers that were generated too, the same way
            clustersSorted = sorted(clusters, key=sum)
            centersSorted  = sorted(centersList, key=sum)
            ### print clustersSorted

            print "\nh2o result, centers (sorted by key=sum)"
            s = '{0:6.2f} {1:6.2f} {2:6.2f}'
            for c in clustersSorted:
                print s.format(*c)
            print "\ngenerated centers (sorted by key=sum)"
            for c in centersSorted:
                print s.format(*c)
            
            for i,center in enumerate(centersSorted):
                # Doing the compare of gen'ed/actual centers is kind of a hamming distance problem.
                # Assuming that the difference between adjacent sums of all center values, 
                # is greater than 2x the sum of all max allowed variance on each value, 
                # Then the sums will be unique and non-overlapping with allowed variance.
                # So a sort of the centers, keyed on sum of all values for a center
                # will create an ordering that can be compared. 
                # sort gen'ed and actual separately.
                # A check on the adjacent center constraint (with allowed variance) is done while gen'ing)
                a = center
                b = clustersSorted[i]
                print "\nexpected:", a
                print "h2o:", b # h2o result
                aStr = ",".join(map(str,a))
                bStr = ",".join(map(str,b))
                iStr = str(i)

                emsg = aStr+" != "+bStr+". Sorted cluster center "+iStr+" x not correct."
                self.assertAlmostEqual(a[0], b[0], delta=ALLOWED_DELTA, msg=emsg)
                emsg = aStr+" != "+bStr+". Sorted cluster center "+iStr+" y not correct."
                self.assertAlmostEqual(a[1], b[1], delta=ALLOWED_DELTA, msg=emsg)
                emsg = aStr+" != "+bStr+". Sorted cluster center "+iStr+" z not correct."
                self.assertAlmostEqual(a[2], b[2], delta=ALLOWED_DELTA, msg=emsg)

            print "Trial #", trial, "completed"

if __name__ == '__main__':
    h2o.unit_main()
