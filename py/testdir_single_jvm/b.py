import sys

import math
OTHER_T = 0.50

# Always need an extra bin (with it's min value) for interpolating
# if we're at the last bin and need to interpolate
# maybe keep a hcnt_min for the high guys for that 
# set this to 1, to see that NUDGE makes sure the end data isn't lost
# should iterate without change, if nudge is big enough
# otherwse  one vlue gets dropped each iteration
# test with BIN_COUNT = 2, to see that it always resolves..i.e. NUDGE is not too big
BIN_COUNT = 1000
sys.path.extend(['.','..','py'])
import h2o_print as h2p, h2o_summ
import numpy as np
import scipy as sp

#****************************************************************************
def findQuantile(d, dmin, dmax, drows, threshold):
    # returns the value at the threshold, or the mean of the two rows that bound it.
    # fixed bin count per pass
    binCount = BIN_COUNT
    maxIterations = 30

    # initial
    newValStart = dmin
    newValEnd   = dmax
    newValRange = newValEnd - newValStart
    newBinCount = binCount # might change, per pass?
    newBinSize  = newValRange / (newBinCount + 0.0)
    newLowCount = 0

    # what if the vals are all constant?
    assert newBinSize != 0

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
        h2p.green_print("threshold", threshold)

        valStart = newValStart
        valEnd   = newValEnd
        valRange = newValRange
        binSize = newBinSize
        lowCount = newLowCount
        # does this relate to the use of 1M in the way the index is created? 
        NUDGE = 1e-3
        NUDGE = (1000 * (valEnd - valStart)) / 1000000
        # ratio it down from binSize
        NUDGE = binSize / binCount
        NUDGE = 0

        for b in range(binCount+1):
            hcnt[b] = 0.0

        # out of the histogram
        hcnt_low = 0
        hcnt_high = 0
        hcnt_high_min = None

        for val in d:
            # need to count the stuff outside the bin-gathering, since threshold compare
            # is based on total row compare
    
            valOffset = val - valStart
            if valOffset < 0:
                hcnt_low += 1
            elif val > valEnd:
                if hcnt_high==0:
                    print "First addition to hcnt_high this pass val:", val, "valEnd:", valEnd
                    # for intepolating past the last bin?
                    hcnt_high_min = val
                else:
                    hcnt_high_min = min(hcnt_high_min, val)
                hcnt_high += 1
            else:
                # where are we zeroing in (start)
                # print valOffset, binSize
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
        currentCnt = newLowCount
        targetCnt = int(math.floor(threshold * drows))
        targetCntFractional = (threshold * drows) - targetCnt
        exactGoal = targetCntFractional==0.0

        print "targetCnt:", targetCnt, "targetCntFractional", targetCntFractional

        while((currentCnt + hcnt[k]) <= targetCnt): 
            currentCnt += hcnt[k]
            k += 1
            assert k <= binCount, "k too large, k: %s binCount %s" % (k, binCount)

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
            print "Guess D:", 'guess', 'k', 'hcnt_min[k]', 'actualBinWidth', 'currentCnt', 'targetCnt', 'hcnt[k]'
            print "Guess D:", guess, k, hcnt_min[k], actualBinWidth, currentCnt, targetCnt, hcnt[k]

        # We should end with a count of 1, otherwise it's still a best guess
        # could be approximately equal
        # THERE CAN BE MULTIPLE VALUES AT THE TARGET VALUE
        # chenk for min = max in that bin!
        print "checking for done, hcnt_min[k]", hcnt_min[k], "hcnt_max[k]", hcnt_max[k]

        # In the right bit with only one value, and the 
        if hcnt_min[k]==hcnt_max[k] and currentCnt==targetCnt: 
            # no mattter what size the fraction it would be on this number
            if hcnt[k]>=2 and targetCntFraction==0:
                done = True
                guess = hcnt_min[k]

        if done:
            print 'Done:', 'hcnt_min[k]', 'hcnt_max[k]', 'currentCnt', 'targetCnt', 'targetCntFractional'
            print 'Done:', hcnt_min[k], hcnt_max[k], currentCnt, targetCnt, targetCntFractional
        else:
            print 'Not Done:', 'hcnt_min[k]', 'hcnt_max[k]', 'currentCnt', 'targetCnt', 'targetCntFractional'
            print 'Not Done:', hcnt_min[k], hcnt_max[k], currentCnt, targetCnt, targetCntFractional

        # do we have to compute the mean, using the current k and nextK bins?
        # if min and max for a bin are different the count must be >1
        if hcnt[k]==1: 
            assert hcnt_min[k]==hcnt_max[k]

        # need to get to 1 entry to 
        # if there's a fractional part, and we're not done, it's in the next k

        # okay...potentially multiple of same value in the bin
        if not done and hcnt[k]>1 and hcnt_min[k]==hcnt_max[k] and targetCntFractional!=0:
            print "\nInterpolating result into single value of this bin"
            print 'Guess E:', 'guess', 'k', 'hcnt[k]', 'hcnt_min[k]', 'hcnt_max[k]', 'currentCnt', 'targetCnt'
            print "Guess E:", guess, k, hcnt[k], hcnt_min[k], hcnt_max[k], currentCnt, targetCnt
            guess = hcnt_min[k]
            done = True

        if not done and hcnt[k]==1 and hcnt_min[k]==hcnt_max[k] and targetCntFractional!=0:
            print "\nSingle value in this bin, but fractional means we need to interpolate to next non-zero"
            print 'k', 'hcnt[k]', 'hcnt_min[k]', 'hcnt_max[k]', 'currentCnt', 'targetCnt'
            print k, hcnt[k], hcnt_min[k], hcnt_max[k], currentCnt, targetCnt

        # one in bin. look to next bin
        if not done and hcnt[k]==1 and targetCntFractional!=0:
            # only legitimate case is !exactGoal?
            assert not exactGoal
            print "Trying to find nextK for possibly interpolating k: %s" % k
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
                nextK = None
                print "nextK is outside the bins. Use hcnt_high_min"
                if hcnt_high > 0:
                    print "\nInterpolating result using hcnt_high_min"
                    print "Guess F with hcnt_high_min:"
                    print 'guess', 'k', 'hcnt_high_min', ' currentCnt', 'targetCnt', 'targetCntFractional', 'hcnt[k]'
                    print guess, k, hcnt_high_min,  currentCnt, targetCnt, targetCntFractional, hcnt[k]
                    guess = (hcnt_max[k]+ hcnt_high_min) / 2.0
                else:
                    print "Guess F with hcnt_high_min not valid:", \
                        guess, k, hcnt_high, hcnt_high_min,  currentCnt, targetCnt, targetCntFractional, hcnt[k]
                    assert false # should never happen?
                    guess = hcnt_high

            else:
                # last might be empty
                assert nextK<binCount, "%s %s" % (nextK, binCount)
                if hcnt[nextK]==0:
                    nextK = k

                nextCnt = int(nextK * binSize)

                # have the "extra bin" for this
                assert nextK < (binCount+1), "nextK too large, nextK: %s binCount %s" % (nextK, binCount)
                    
                print "k:", k, "nextK", nextK    
                if  k != nextK:
                    guess = (hcnt_max[k] + hcnt_min[nextK]) / 2.0
                    print "\nInterpolating result using nextK"
                    print "Guess G with nextK:", guess, k, nextK,  hcnt_max[k], hcnt_min[nextK], currentCnt, targetCnt, hcnt[k]

                # since we moved into the partial bin
                assert hcnt[nextK]!=0, hcnt[nextK]
                assert hcnt[k]!=0, hcnt[k]

            # now we're done

        newValStart = hcnt_min[k] - NUDGE# FIX! should we nudge a little?
        newValEnd   = hcnt_max[k] + NUDGE # FIX! should we nudge a little?
        newValRange = newValEnd - newValStart 
        newBinSize = newValRange / (binCount + 0.0)

        # assert done or newBinSize!=0
        if not done:
            print "Saying done because newBinSize is 0."
            print "newValRange: %s, hcnt[k]: %s hcnt_min[k]: %s hcnt_max[k]: %s" %\
                 (newValRange, hcnt[k], hcnt_min[k], hcnt_max[k])
            done = newBinSize==0
        # if we have to interpolate
        # if it falls into this bin, interpolate to this bin means one answer?

        # cover the case above with multiple entris in a bin, all the same value
        # will be zero on the last pass?
        # assert newBinSize != 0 or done

        # need the count up to but not including newValStart
        newLowCount = currentCnt
        best_result.append(guess)
        print "Compare these two, should be identical? %s %s" % (guess, best_result[-1])
        
        iteration += 1
        h2p.blue_print("Ending Pass", iteration, "best_result:", best_result, "done:", done, "hcnt[k]", hcnt[k])
        print "currentCnt", currentCnt, "targetCnt", targetCnt, "hcnt_low", hcnt_low, "hcnt_high", hcnt_high
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
# csvPathname = './d.csv'
csvPathname = './syn_binary_1000000x1.csv'
# csvPathname = './syn_binary_100x1.csv'
col = 0

print "Reading csvPathname"
dataset = np.genfromtxt(
    open(csvPathname, 'r'),
    delimiter=',',
    # skip_header=1,
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
thresholdList = [OTHER_T]

quantiles = findQuantileList(d, dmin, dmax, drows, thresholdList)
#*****************************************************************
# for comparison
#*****************************************************************
# perPrint = ["%.2f" % v for v in a]

from scipy import stats
a1 = stats.scoreatpercentile(target, per=100*OTHER_T, interpolation_method='fraction')
h2p.red_print("stats.scoreatpercentile:", a1)
a2 = stats.mstats.mquantiles(targetFP, prob=[OTHER_T])
h2p.red_print("scipy stats.mstats.mquantiles:", ["%.2f" % v for v in a2])

# looking at the sorted list here
targetFP.sort()
b = h2o_summ.percentileOnSortedList(targetFP, OTHER_T)
h2p.blue_print( "from scipy:", a2)

