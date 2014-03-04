import sys

import math
DO_MEDIAN = True
DO_TO_BEFORE = False
sys.path.extend(['.','..','py'])
import h2o_print as h2p, h2o_summ
import numpy as np
import scipy as sp

#****************************************************************************
def findQuantile(d, dmin, dmax, drows, threshold):
    # returns the value at the threshold, or the mean of the two rows that bound it.
    # fixed bin count per pass
    binCount = 50
    maxIterations = 30

    # initial
    newValStart = dmin
    newValEnd   = dmax
    newValRange = newValEnd - newValStart
    newBinCount = binCount # might change, per pass?
    newBinSize  = newValRange / (newBinCount + 0.0)
    newLowCount = 0

    # check if val is NaN and ignore?
    # floor so we end up at the target if the row exists
    # otherwise one below where we'll interpolate
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
            # need to count the stuff outside the bin-gathering, since threshold compare
            # is based on total row compare
    
            valOffset = val - valStart
            if valOffset < 0:
                hcnt_low += 1
            elif val > valEnd:
                hcnt_high += 1
            else:
                # where are we zeroing in (start)
                hcntIdx = int(round((valOffset * 1000000.0) / binSize) / 1000000.0)
                assert hcntIdx >=0 and hcntIdx<=binCount, "val %s %s %s %s hcntIdx: %s binCount: %s binSize: %s" % \
                    (val, valStart, valEnd, valOffset, hcntIdx, binCount, binSize)

                if hcnt[hcntIdx]==0:
                    hcnt_min[hcntIdx] = val
                    hcnt_max[hcntIdx] = val
                else:
                    hcnt_min[hcntIdx] = min(hcnt_min[hcntIdx], val)
                    hcnt_max[hcntIdx] = max(hcnt_max[hcntIdx], val)

                hcnt[hcntIdx] += 1

        # everything should either be in low, the bins, or high
        ht = htot2()
        assert drows == ht, "drows: %s htot2() %s not equal" % (drows, ht) 

        # now walk thru and find out what bin you look at it's valOffset (which will be the hcnt_min for that bin
        s = 0
        k = 0
        prevK = 0
        currentCnt = newLowCount
        targetCnt = int(math.floor(threshold * drows))
        if DO_TO_BEFORE:
            e = lambda x, y : x < y
        else:
            e = lambda x, y : x <= y
        while( e((currentCnt + hcnt[k]), targetCnt) ): 
            currentCnt += hcnt[k]
            if hcnt[k]!=0:
                prevK = k # will always be the previous non-zero (except when k=0)
            k += 1
            assert k <= binCount, "k too large, k: %s binCount %s" % (k, binCount)

        assert (k==0 and prevK==0) or prevK<k, "prevK should be before k except if both are zero %s %s" % (prevK, k)

        # I guess we don't care about the values at the bin edge
        # binLeftEdge = valStart + k*binSize
        # binRightEdge = valStart + k*binSize
        # this might fail if there are fp issues, but will show we might need smudging on the bin boundaries or ??
        if s==targetCnt or hcnt[k]==0:
            if hcnt[k]!=0:
                guess = hcnt_min[k]
                h2p.red_print ("Guess A", guess, currentCnt, targetCnt)
            else:
                if k==0:
                    assert hcnt[k+1]!=0  # "Unexpected state of starting hcnt bins"
                    guess = hcnt_min[k+1] # use the first value in the next bin
                    h2p.red_print ("Guess B", guess, currentCnt, targetCnt)
                else:
                    if hcnt[k-1]!=0:
                      guess = hcnt_max[k-1] # use the last value in the prior bin
                      h2p.red_print ("Guess C", guess, currentCnt, targetCnt)
                    else:
                      assert false  # "Unexpected state of adjacent hcnt bins"
        else:
            # nonzero hcnt[k] guarantees these are valid
            h2p.red_print("hcnt_max[k]", hcnt_max[k], "hcnt_min[k]", hcnt_min[k])
            actualBinWidth = hcnt_max[k] - hcnt_min[k]
            assert actualBinWidth <= binSize

            # interpolate within the populated bin, assuming linear distribution
            # since we have the actual min/max within a bin, we can be more accurate
            # compared to using the bin boundaries
            # Note actualBinWidth is 0 when all values are the same in a bin
            # Interesting how we have a gap that we jump between max of one bin, and min of another.
            guess = hcnt_min[k] + (actualBinWidth * ((targetCnt - currentCnt)/ hcnt[k]))
            print "Guess D:", guess, k, hcnt_min[k], actualBinWidth, currentCnt, targetCnt, hcnt[k]

        # We should end with a count of 1, otherwise it's still a best guess
        # could be approximately equaly
        # THERE CAN BE MULTIPLE VALUES AT THE TARGET VALUE
        # chenk for min = max in that bin!
        print "checking for done, hcnt_min[k]", hcnt_min[k], "hcnt_max[k]", hcnt_max[k]

        if DO_TO_BEFORE:
            done = hcnt_min[k]==hcnt_max[k] and (currentCnt+hcnt[k])==targetCnt
        else:
            done = hcnt_min[k]==hcnt_max[k] and currentCnt==targetCnt

        

        # do we have to compute the mean, using the current k and nextK bins?

        # if min and max for a bin are different the count must be >1
        assert (hcnt[k]==1 and hcnt_min[k]==hcnt_max[k]) or hcnt[k]>1

        if not done and hcnt[k]==1: # need mean with next_k
            # always figure nextK
            # should we take the mean of the current and next non-zero bin
            # find the next non-zero bin too
            if k<binCount:
                nextK = k + 1 # could put it over binCount
            else:
                nextK = k
            
            while nextK<binCount and hcnt[nextK]==0:
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
                print "\nInterpolating result"
                print "Guess E:", guess, k, nextK,  hcnt_max[k], hcnt_min[nextK], actualBinWidth,\
                    currentCnt, targetCnt, hcnt[k]

            assert hcnt[nextK]!=0, hcnt[nextK]
                
            assert hcnt[k]!=0, hcnt[k]
            assert hcnt[prevK]!=0, hcnt[prevK]

            # now we're done
            done = True

        # s is right before targetCnt (less than the bin width..good for avoiding rounding error issues?
        # we should also add one of the new binsizes to the end, to avoid missing something 
        # (but that's a waste..just need to 
        # bump it a little? only an issue, if the new end is less than max. 
        # Could add or subtract a small number to make sure?
        # just live with it for now.

        # adjust the start to be that value
        # subtract something to cover fp error?
        # keep one bin below that 

        # or maybe, just grab hcnt_min from the next k+1 bi and hcnt_max from k-1
        # that'll git er done.



        
        # can't nudge it a little bigger? end condition prevents 
        # we have a one element end condition. I suppose we could have a two element end condition
        # but we'd have to adjust the two element end condition and the two element mean calculation
        # if needed, for that end condition. Not clear it saves a pass, since we tend to jump from 1000 in bin to 1?
        newValStart = hcnt_min[k] # FIX! should we nudge a little?
        newValEnd   = hcnt_max[k] # FIX! should we nudge a little?
        newValRange = newValEnd - newValStart 
        newBinSize = newValRange / binCount

        # need the count up to but not including newValStart
        newLowCount = currentCnt
        best_result.append(guess)
        print "Compare these two, should be identical? %s %s" % (guess, best_result[-1])
        
        iteration += 1
        h2p.blue_print("Ending Pass", iteration, "best_result:", best_result, "done:", done, "hcnt[k]", hcnt[k])
        print "currentCnt", currentCnt, "targetCnt", targetCnt
        print "was", valStart, valEnd, valRange, binSize
        print "next", newValStart, newValEnd, newValRange, newBinSize

    return best_result[-1]

#****************************************************************************
def findQuantileList(d, dmin, dmax, drows, thresholdList):
    # returns val or list of vals
    q = []
    for threshold in thresholdList:
        q.append(findQuantile(d, dmin, dmax, drows, threshold))
    return q

#****************************************************************************

def twoDecimals(l): 
    if isinstance(l, list):
        return ["%.2f" % v for v in l] 
    else:
        return "%.2f" % l

# csvPathname = './syn_binary_1000000x1.csv'
csvPathname = './syn_binary_100000x1.csv'
# csvPathname = './syn_binary_100x1.csv'
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

#*****************************************************************
# h2o
#*****************************************************************
d = target
dmin = min(d)
dmax = max(d)
drows = len(d)
if DO_MEDIAN:
    thresholdList = [0.5]
else:
    thresholdList = [0.999]

quantiles = findQuantileList(d, dmin, dmax, drows, thresholdList)
#*****************************************************************
# for comparison
#*****************************************************************
# perPrint = ["%.2f" % v for v in a]
per = [1 * t for t in thresholds]
print "scipy per:", per

from scipy import stats
a1 = stats.scoreatpercentile(target, per=100*(0.50 if DO_MEDIAN else 0.999), interpolation_method='fraction')
h2p.red_print("stats.scoreatpercentile:", a1)
a2 = stats.mstats.mquantiles(targetFP, prob=per)
h2p.red_print("scipy stats.mstats.mquantiles:", ["%.2f" % v for v in a2])

# looking at the sorted list here
targetFP.sort()
b = h2o_summ.percentileOnSortedList(targetFP, 0.50 if DO_MEDIAN else 0.999)
label = '50%' if DO_MEDIAN else '99.9%'
h2p.blue_print(label, "from scipy:", a2[5 if DO_MEDIAN else 10])

a3 = stats.mstats.mquantiles(targetFP, prob=per)
h2p.red_print("after sort")
h2p.red_print("scipy stats.mstats.mquantiles:", ["%.2f" % v for v in a3])

