import time, sys, random, math
sys.path.extend(['.','..','py'])

# a truly uniform sphere
# http://stackoverflow.com/questions/5408276/python-uniform-spherical-distribution
# he offers the exact solution: http://stackoverflow.com/questions/918736/random-number-generator-that-produces-a-power-law-distribution/918782#918782
# In spherical coordinates, taking advantage of the sampling rule:
# http://stackoverflow.com/questions/2106503/pseudorandom-number-generator-exponential-distribution/2106568#2106568

CLUSTERS = 15
# GB_SPHERE_PTS = 1000000 # 1GB
SPHERE_PTS = 100000
RANDOMIZE_SPHERE_PTS = False
JUMP_RANDOM_ALL_DIRS = True
SHUFFLE_SPHERES = False
RADIUS_NOISE = True
ALLOWED_CENTER_DELTA = 1
MAX_DIGITS_IN_DIMENSIONS = [2,1,3,4,8,5]
MAXINTS = [(pow(10,d) - 1) for d in MAX_DIGITS_IN_DIMENSIONS]
print "MAXINTS per col:", MAXINTS
DIMENSIONS = len(MAX_DIGITS_IN_DIMENSIONS)

# don't include the unnecessary columns
JUST_SPHERES = True
DO_REALS = True

def getInterestingEnum():
    # powerhouse data
    # U0000000001070000E1300000000R50000000,07,4,277,1250,10000013,11400]
    # U0000000001070000 (16)
    # E1300000000 (10)
    # R50000000 (8)
    u = "U" + str(random.randint(1, pow(10,16)) - 1)
    e = "E" + str(random.randint(1, pow(10,10)) - 1)
    r = "R" + str(random.randint(1, pow(10,8))  - 1)
    return u + e + r

    # pts have 6 dimensions
    # 07 (2) (0-99)
    # 4, (1)  (0-9)
    # 277, (3)  (0-999)
    # 1250, (4)  (0-9999)
    # 10000013, (8) (0-99999999)
    # 11400 (5) (0-99999)

def get_xyz_sphere(R):
    u = random.random() # 0 to 1
    # add a little noise
    r = R * (u ** (1.0/3))
    if RADIUS_NOISE:
        rNoise = random.random() * .1 * r
        r += rNoise

    costheta = random.uniform(-1,1)
    theta = math.acos(costheta)

    phi = random.uniform(0, 2 * math.pi)
    # now you have a (r, theta, phi) group which can be transformed to (x, y, z) 
    x = r * math.sin(theta) * math.cos(phi)
    y = r * math.sin(theta) * math.sin(phi)
    z = r * math.cos(theta) 
    # use the extra 0 cases for jump dimensions? (picture "time" and other dimensions)
    # randomly shift the sphere xyz across the allowed dimension?
    xyzzy = [x, y, z]
    return xyzzy

def write_spheres_dataset(csvPathname, CLUSTERS, n):
    dsf = open(csvPathname, "w+")

    # going to do a bunch of spheres, with differing # of pts and R
    # R is radius of the spheres
    # separate them by 3 * the previous R
    # keep track of the centers so we compare to a sorted result from H2O
    centersList = []
    currentCenter = None
    totalRows = 0
    sphereCnt = 0
    # we might do more iterations if we get a "bad" center
    for sphereCnt in range(CLUSTERS):
        R = 10 * (sphereCnt+1)
        # FIX! problems if we don't jump the other dimensions?
        # try jumping in all dimensions
        # newOffset[xyzChoice] = jump


        # build a sphere at that center
        # fixed number of pts?
        if RANDOMIZE_SPHERE_PTS:
            # pick a random # of points, from .5n to 1.5n
            numPts = random.randint(int(.5*n), int(1.5*n))
        else:
            numPts = n

        if DIMENSIONS < 3:
            raise Exception("DIMENSIONS must be >= 3, is:"+DIMENSIONS)
        xyzShift = random.randint(0,DIMENSIONS-3)
        print "xyzShift:", xyzShift
        
        # some random place in the allowed space. Let's just assume we don't get 
        # sphere overlap that kills us. With enough dimensions, that might be true?
        # looks like we compare the sum of the centers above (in -1 to 1 space)
	if DO_REALS:
            initial = [(0.5 * random.random() * MAXINTS[i]) for i in range(DIMENSIONS)]
	else:
            initial = [int((0.5 * random.randint(0,MAXINTS[i]))) for i in range(DIMENSIONS)]

        # zero out the ones we'll mess with just to be safe/clear
        # this will be the initial center? make it within the range of half
        # the allowed int
        scale = []
        for i in range(3):
            MAX = MAXINTS[xyzShift+i]
            a = int(0.5 * random.randint(0,MAX) + R) # R plus something
            if random.randint(0,1):
                a = -a
            initial[xyzShift+i] = a
    
            # randomly sized "ball"  to scale our "sphere"
            # use only 1/2 the allowed range, letting the center skew by one half also
	    if DO_REALS:
            	scale.append(0.5 * random.randint(0,MAX))
	    else:
            	scale.append(0.5 * random.random() * MAX);

        # figure out the next center
        if currentCenter is None:
            currentCenter = initial[:]
            lastCenter = currentCenter[:]
        else:
            currentCenter = initial[:]
            delta = [a-b for a, b in zip(lastCenter, currentCenter)]
            maxDelta = max(delta)
            # R is always getting bigger, so use the current R to check
            if maxDelta < (ALLOWED_CENTER_DELTA * R):
                print "currentCenter:", currentCenter, "lastCenter:", lastCenter
                print "ERROR: adjacent centers are too close for our sort algorithm %s %s" % (maxDelta, R)
                continue
            lastCenter = currentCenter[:]
                
        centersList.append(currentCenter)
        print "currentCenter:", currentCenter, "R:", R, "numPts", numPts

        for n in range(numPts):
            thisPt = currentCenter[:]
            xyz = get_xyz_sphere(R) 
            for i in range(3):
		if DO_REALS:
                   thisPt[xyzShift+i] += xyz[i]
		else:
                   thisPt[xyzShift+i] += int(xyz[i])

	    if JUST_SPHERES:
                row = ",".join(map(str,thisPt)) + "\n"
	    else:
                interestingEnum = getInterestingEnum()
                row = ",".join(map(str,[interestingEnum] + thisPt)) + "\n"
            dsf.write(row)
            totalRows += 1

	sphereCnt += 1 # end of while loop

    dsf.close()
    print "Spheres created:", len(centersList), "totalRows:", totalRows
    return centersList


#*****************************************************
csvFilename = 'syn_sphere_gen.csv'
csvPathname = './' + csvFilename
centersList = write_spheres_dataset(csvPathname, CLUSTERS, SPHERE_PTS)

if SHUFFLE_SPHERES:
    # since we create spheres in order
    csvFilename2 = 'syn_sphere_gen_shuffled.csv'
    csvPathname2 = './' + csvFilename2
    import h2o_util
    h2o_util.file_shuffle(csvPathname, csvPathname2)
else:
    csvFilename2 = csvFilename
    csvPathname2 = csvPathname
