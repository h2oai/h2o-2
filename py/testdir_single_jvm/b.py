import sys
import math
sys.path.extend(['.','..','py'])
import h2o_print as h2p, h2o_summ

DO_MEDIAN = True

import numpy as np
import scipy as sp

def twoDecimals(l): 
    if isinstance(l, list):
        return ["%.2f" % v for v in l] 
    else:
        return "%.2f" % l

csvPathname = './syn_binary_100x1.csv'
csvPathname = './syn_binary_1000000x1.csv'
col = 0

print "Reading csvPathname"
dataset = np.genfromtxt(
    open(csvPathname, 'r'),
    delimiter=',',
    skip_header=1,
    dtype=None) # guess!

print dataset.shape
# target = [x[col] for x in dataset]
# one column
target = dataset
targetFP = np.array(target, np.float)

n_features = len(dataset) - 1
print "n_features:", n_features

print "histogram of target"
print target
print sp.histogram(target)

thresholds   = [0.001, 0.01, 0.1, 0.25, 0.33, 0.5, 0.66, 0.75, 0.9, 0.99, 0.999]
# perPrint = ["%.2f" % v for v in a]
per = [1 * t for t in thresholds]
print "scipy per:", per

from scipy import stats
a1 = stats.scoreatpercentile(target, per=0.5)
h2p.red_print("stats.scoreatpercentile:", a1)
a2 = stats.mstats.mquantiles(targetFP, prob=per)
h2p.red_print("scipy stats.mstats.mquantiles:", ["%.2f" % v for v in a2])

targetFP.sort()
b = h2o_summ.percentileOnSortedList(targetFP, 0.50 if DO_MEDIAN else 0.999)
label = '50%' if DO_MEDIAN else '99.9%'
h2p.blue_print(label, "from scipy:", a2[5 if DO_MEDIAN else 10])

a3 = stats.mstats.mquantiles(targetFP, prob=per)
h2p.red_print("after sort")
h2p.red_print("scipy stats.mstats.mquantiles:", ["%.2f" % v for v in a3])

d = target

# fixed bin count per pass
binCount = 1000
dmin = min(d)
dmax = max(d)
drows = len(d)

maxIterations = 10
threshold = 0.50

# initial
newValStart = dmin
newValEnd   = dmax
newValRange = newValEnd - newValStart
newBinCount = binCount # might change, per pass?
newBinSize = newValRange / (newBinCount + 0.0)
newLowCount = 0

# check if val is NaN and ignore?
targetCount = math.floor(threshold * drows)
minK = 0
maxK = binCount - 1

# break out on stopping condition
# reuse the histogram array hcnt[]
iteration = 0
done = False
# always have one more due to round?
hcnt = [None for b in range(binCount+1)]
hcnt_min = [None for b in range(binCount+1)]
hcnt_max = [None for b in range(binCount+1)]
hcnt_low = 0
hcnt_high = 0

best_result = []

def htot2():
    return sum(hcnt) + hcnt_low + hcnt_high
    
while iteration < maxIterations and not done:
    h2p.green_print("newValStart", newValStart)
    h2p.green_print("newValEnd", newValEnd)
    h2p.green_print("newValRange", newValRange)
    h2p.green_print("newBinSize", newBinSize)
    h2p.green_print("newLowCount", newLowCount)

    valStart = newValStart
    valEnd   = newValEnd
    valRange = newValRange
    binSize = newBinSize
    lowCount = newLowCount

    for b in range(binCount+1):
        hcnt[b] = 0.0

    # out of the histogram
    hcnt_low = 0
    hcnt_high = 0

    for val in d:

        if val < valStart:
            hcnt_low += 1
        elif val > valEnd:
            hcnt_high += 1
        else:
            # where are we zeroing in (start)
            valOffset = val - valStart
            # print "val:", val, "valStart:", valStart, "valOffset:", valOffset
            hcntIdx = int(round((valOffset * 1000000.0) / binSize) / 1000000)
            # print "hcntIdx:", hcntIdx
            assert hcntIdx >=0 and hcntIdx<=binCount

            if hcnt[hcntIdx]==0:
                hcnt_min[hcntIdx] = val
                hcnt_max[hcntIdx] = val
            else:
                hcnt_min[hcntIdx] = min(hcnt_min[hcntIdx], val)
                hcnt_max[hcntIdx] = max(hcnt_max[hcntIdx], val)

            hcnt[hcntIdx] += 1

    # The bins should never lose any data
    ht = htot2()
    assert drows == ht, "drows: %s htot2() %s not equal" % (drows, htot2) 

    # now walk thru and find out what bin you look at it's valOffset (which will be the hcnt_min for that bin
    s = 0
    k = 0
    prevK = 0
    currentCnt = newLowCount
    targetCnt = int(math.floor(threshold * drows))
    while( (currentCnt + hcnt[k]) < targetCnt):
        currentCnt += hcnt[k]
        if hcnt[k]!=0:
            prevK = k # will always be the previous non-zero (except when k=0)
        k += 1
        assert k <= binCount, "k too large, k: %s binCount %s" % (k, binCount)

    #     binLeftEdge[b] = binStart + b*binSize
    if s==targetCnt or hcnt[k]==0:
        if hcnt[k]!=0:
            guess = hcnt_min[k]
            h2p.red_print ("Guess A", guess, currentCnt, targetCnt)
        else:
            if k==0:
                assert hcnt[k+1]!=0  # "Unexpected state of starting hcnt bins"
                guess = hcnt_min[k+1]
                h2p.red_print ("Guess B", guess, currentCnt, targetCnt)
            else:
                if hcnt[k-1]!=0:
                  guess = hcnt_max[k-1]
                  h2p.red_print ("Guess C", guess, currentCnt, targetCnt)
                else:
                  assert false  # "Unexpected state of adjacent hcnt bins"
    else:
        # nonzero hcnt[k] guarantees these are valid
        h2p.red_print("hcnt_max[k]", hcnt_max[k], "hcnt_min[k]", hcnt_min[k])
        actualBinWidth = hcnt_max[k] - hcnt_min[k]

        # interpolate within the populated bin, assuming linear distribution
        # since we have the actual min/max within a bin, we can be more accurate
        # compared to using the bin boundaries
        # Note actualBinWidth is 0 when all values are the same in a bin
        # Interesting how we have a gap that we jump between max of one bin, and min of another.
        guess = hcnt_min[k] + (actualBinWidth * ((targetCnt - currentCnt)/ hcnt[k]))
        print "Guess D:", guess, k, hcnt_min[k], actualBinWidth, currentCnt, targetCnt, hcnt[k]

    # we should end with a count of 1, otherwise it's still a best guess
    # could be approximately equaly
    # THERE CAN BE MULTIPLE VALUES AT THE TARGET VALUE
    # chenk for min = max in that bin!
    print "checking for done, hcnt_min[k]", hcnt_min[k], "hcnt_max[k]", hcnt_max[k]
    done = hcnt_min[k]==hcnt_max[k] and (currentCnt+hcnt[k])==targetCnt

    # should we take the mean of the current and next non-zero bin
    # find the next non-zero bin too
    if k<binCount:
        nextK = k + 1 # could put it over binCount
    else:
        nextK = k
    
    while( nextK<binCount and hcnt[nextK]==0):
        nextK += 1

    if nextK>=binCount:
        print "k must be the last non-zero bin. set nextK to last bin"
        nextK = binCount - 1 

    # last might be empty
    assert nextK<binCount, "%s %s" % (nextK, binCount)
    if hcnt[nextK]==0:
        nextK = k

    nextCnt = int(nextK * binSize)

    # have the "extra bin" for this
    assert nextK < (binCount+1), "nextK too large, nextK: %s binCount %s" % (nextK, binCount)
    if k != nextK:
        guess = (hcnt_max[k] + hcnt_min[nextK]) / 2.0
        
    best_result.append(guess)

    assert hcnt[nextK] != 0, hcnt[nextK]
    assert hcnt[k] != 0, hcnt[k]

    # s is right before targetCnt (less than the bin width..good for avoiding rounding error issues?
    # we should also add one of the new binsizes to the end, to avoid missing something (but that's a waste..just need to 
    # bump it a little? only an issue, if the new end is less than max. Could add or subtract a small number to make sure?
    # just live with it for now.

    # adjust the start to be that value
    # subtract something to cover fp error?
    # keep one bin below that 

    # or maybe, just grab hcnt_min from the next k+1 bi and hcnt_max from k-1
    # that'll git er done.
    if k==minK:
        newValStart = hcnt_min[k] # FIX! should we nudge a little?
    else:
        newValStart = hcnt_min[k] # need to make forward progress?
        # newValStart = hcnt_max[prevK] # prev non-zero bin

    if k==maxK:
        newValEnd   = hcnt_max[k] # FIX! should we nudge a little?
    else:
        # newValEnd   = hcnt_min[nextK] # next non-zero bin
        newValEnd   = hcnt_max[k] # need to make forward progress

    newValRange = newValEnd - newValStart
    newBinSize = newValRange / binCount

    # need the count up to but not including newValStart
    newLowCount = currentCnt
    iteration += 1
    
    print "Starting Iteration", iteration, "best_result:", best_result, "done:", done, "hcnt[k]", hcnt[k]
    print "currentCnt", currentCnt, "targetCnt", targetCnt
    print "was", valStart, valEnd, valRange, binSize
    print "next", newValStart, newValEnd, newValRange, newBinSize
