import sys
sys.path.extend(['.','..','py'])
import h2o_print as h2p, h2o_summ
import numpy as np
import scipy as sp
import math
OTHER_T = 0.50
BIN_COUNT = 100000

# might have multiple rows with that one number
# possible answers:
#   that number if single row  in that bin, 
#   that number if the threshold is fractional and 2 or more rows in that bin
#   if 1 row in the last bin, have to interpolate (mean), to the next sorted value 
#   this will be the min of the "outside the bin" 
#   (min of everthing to the right of the last bin)
# When doing the repeated binning, may have to "nudge" the computed
# bin edges, to make sure we don't lose the target row due to finite precision fp issues

# interesting tests
# set bin count to 1, and it should iterate forever, without resolving anything
# shouldn't lose anything from the bin due to edge leakage
# set bin count to 2, so

# continous distributions of data are the easy cases
# best test: pick a target value, than add data < and > it with the right percentages
# for your target threshold. Answer should be your target value, exactly.

# The interpolation cases are harder to generate the test case for.
# sometimes it's best to sort the list, and do the interpolation by hand, for generating
# the expected value

# memory is conserved by reusing the histogram each pass. histogram has per bin cnt/min/max
# some other bits of state are used per iteration, but not per bin.

# Can process multiple histograms with different binning goals, per data pass. 
# This does one (hcnt2) # histogram (could pass a list  of hcnts and associated state)

# each pass creates a best guess from the info available at that pass
# best_result[] list then shows the incremental improvement per pass 

#****************************************************************************
def findQuantile(d, dmin, dmax, threshold):
    # return the value at the threshold, or the mean of the two rows that bound it.
    # fixed bin count per pass. Stops at maxIterations if not resolved to one true answer
    maxIterations = 30

    # totalRows should be cleansed of NAs. assume d doesn't have NAs (cleaned elsewhere)
    totalRows = len(d)
    # Used to have 
    desiredBinCnt = BIN_COUNT
    maxBinCnt = desiredBinCnt + 1 # might go one over due to FP issues

    # initialize
    newValStart = dmin
    newValEnd   = dmax
    newValRange = newValEnd - newValStart
    desiredBinCnt = BIN_COUNT # Could do per-pass adjustment, but fixed works fine.
    newBinSize  = newValRange / (desiredBinCnt + 0.0)
    newLowCount = 0 # count of rows below the bins
    # yes there is no newHighCount. Created during the pass, though.

    # state shared by each pass
    assert maxBinCnt > 0

    hcnt = [None for b in range(maxBinCnt)]
    hcnt_min = [None for b in range(maxBinCnt)]
    hcnt_max = [None for b in range(maxBinCnt)]
    hcnt_low = 0
    hcnt_high = 0

    assert newBinSize != 0 # can be negative
    assert newValEnd > newValStart
    assert newValRange > 0

    # break out on stopping condition
    # reuse the histogram array hcnt[]
    iteration = 0
    done = False
    # append to a list of best guesses per pass
    best_result = []

    def htot2():
        return sum(hcnt) + hcnt_low + hcnt_high
        
    while iteration <= maxIterations and not done:
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
        desiredBinCnt = BIN_COUNT
        maxBinCnt = desiredBinCnt + 1 # might go one over due to FP issues

        # playing with creating relative NUDGE values to make sure bin range
        # is always inclusive of target.
        # ratio it down from binSize. 
        # It doesn't need to be as big as binSize.
        # implicitly, it shouldn't need to be as large as binSize
        NUDGE = 0

        # init to zero for each pass
        for b in range(maxBinCnt):
            hcnt[b] = 0.0

        # Init counts outside of the bins
        hcnt_low = 0
        hcnt_high = 0

        # minimum value for higher than the bin. Needed for interpolation
        hcnt_high_min = None

        for val in d:
            # Need to count the stuff outside the bin-gathering, 
            # since threshold compare is based on total row compare
            valOffset = val - valStart
            if valOffset < 0:
                hcnt_low += 1
            elif val > valEnd:
                if hcnt_high==0:
                    hcnt_high_min = val
                else:
                    hcnt_high_min = min(hcnt_high_min, val)
                hcnt_high += 1
            else:
                # where are we zeroing in? (start)
                # print valOffset, binSize
                hcntIdx = int(math.floor((valOffset * 1000000.0) / binSize) / 1000000.0)
                assert hcntIdx >=0 and hcntIdx<=maxBinCnt, "val %s %s %s %s hcntIdx: %s maxBinCnt: %s binSize: %s" % \
                    (val, valStart, valEnd, valOffset, hcntIdx, maxBinCnt, binSize)

                if hcnt[hcntIdx]==0:
                    hcnt_min[hcntIdx] = val
                    hcnt_max[hcntIdx] = val
                else:
                    hcnt_min[hcntIdx] = min(hcnt_min[hcntIdx], val)
                    hcnt_max[hcntIdx] = max(hcnt_max[hcntIdx], val)

                hcnt[hcntIdx] += 1

        # everything should either be in low, the bins, or high
        totalBinnedRows = htot2()
        assert totalRows==totalBinnedRows, "totalRows: %s htot2() %s not equal" % (totalRows, totalBinnedRows) 

        # now walk thru and find out what bin to look inside
        currentCnt = hcnt_low
        targetCntFull = (threshold * totalRows) + 0.5
        targetCntInt = int(math.floor(targetCntFull))
        targetCntFract = targetCntFull - targetCntInt

        print "targetCntInt:", targetCntInt, "targetCntFract", targetCntFract

        k = 0
        while((currentCnt + hcnt[k]) < targetCntInt): 
            currentCnt += hcnt[k]
            k += 1
            assert k<=maxBinCnt, "k too large, k: %s maxBinCnt %s" % (k, maxBinCnt)

        if hcnt[k]==1: 
            assert hcnt_min[k]==hcnt_max[k]

        # some possibily interpolating guesses first, in guess we have to iterate (best guess)
        done = False
        guess = (hcnt_max[k] - hcnt_min[k]) / 2

        if currentCnt==targetCntInt:
            if hcnt[k]>2:
                guess = hcnt_min[k]
                done = True
                print "Guess A", guess

            if hcnt[k]==2:
                # no mattter what size the fraction it would be on this number
                guess = (hcnt_max[k] + hcnt_min[k]) / 2.0
                done = True
                print "Guess B", guess

            if hcnt[k]==1 and targetCntFract==0:
                assert hcnt_min[k]==hcnt_max[k]
                guess = hcnt_min[k]
                done = True
                print "k", k
                print "Guess C", guess

            if hcnt[k]==1 and targetCntFract!=0:
                assert hcnt_min[k]==hcnt_max[k]
                print "\nSingle value in this bin, but fractional means we need to interpolate to next non-zero"
                if k<maxBinCnt:
                    nextK = k + 1 # could put it over maxBinCnt
                else:
                    nextK = k
                while nextK<maxBinCnt and hcnt[nextK]==0:
                    nextK += 1

                # have the "extra bin" for this
                if nextK >= maxBinCnt:
                    assert hcnt_high!=0
                    print "hello1:", hcnt_high_min
                    nextVal = hcnt_high_min
                else:
                    print "hello2:", nextK
                    assert hcnt[nextK]!=0
                    nextVal = hcnt_min[nextK]

            guess = (hcnt_max[k] + nextVal) / 2.0
            done = True # has to be one above us when needed. (or we're at end)
            print 'k', 'hcnt_max[k]', 'nextVal'
            print "hello3:", k, hcnt_max[k], nextVal
            print "\nInterpolating result using nextK: %s nextVal: %s" % (nextK, nextVal)
            print "Guess D", guess

        if not done:
            newValStart = hcnt_min[k] - NUDGE # FIX! should we nudge a little?
            newValEnd   = hcnt_max[k] + NUDGE # FIX! should we nudge a little?
            newValRange = newValEnd - newValStart 
            
            # maxBinCnt is always binCount + 1, since we might cover over due to rounding/fp issues?
            newBinSize = newValRange / (desiredBinCnt + 0.0)
            newLowCount = currentCnt
            # assert done or newBinSize!=0 and live with current guess
            print "Saying done because newBinSize is 0."
            print "newValRange: %s, hcnt[k]: %s hcnt_min[k]: %s hcnt_max[k]: %s" %\
                 (newValRange, hcnt[k], hcnt_min[k], hcnt_max[k])

            if newBinSize==0:
                guess = newValStart
                done = True

            # if we have to interpolate
            # if it falls into this bin, interpolate to this bin means one answer?

            # cover the case above with multiple entris in a bin, all the same value
            # will be zero on the last pass?
            # assert newBinSize != 0 or done
            # need the count up to but not including newValStart

        best_result.append(guess)
        print "Compare these two, should be identical? %s %s" % (guess, best_result[-1])
        
        iteration += 1
        h2p.blue_print("Ending Pass", iteration)
        h2p.blue_print("best_result:", best_result, "done:", done, "hcnt[k]", hcnt[k])
        print "currentCnt", currentCnt, "targetCntInt", targetCntInt, "hcnt_low", hcnt_low, "hcnt_high", hcnt_high
        print "was", valStart, valEnd, valRange, binSize
        print "next", newValStart, newValEnd, newValRange, newBinSize

    return best_result[-1]

#****************************************************************************
def findQuantileList(d, dmin, dmax, thresholdList):
    q = []
    for threshold in thresholdList:
        q.append(findQuantile(d, dmin, dmax, threshold))
    return q

#****************************************************************************

def twoDecimals(l): 
    if isinstance(l, list):
        return ["%.2f" % v for v in l] 
    else:
        return "%.2f" % l

# csvPathname = './syn_binary_1000000x1.csv'
csvPathname = './syn_binary_100x1.csv'
csvPathname = './d.csv'
csvPathname = './syn_binary_100000x1.csv'
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
thresholdList = [OTHER_T]

quantiles = findQuantileList(d, dmin, dmax, thresholdList)
#*****************************************************************
# for comparison
#*****************************************************************
# perPrint = ["%.2f" % v for v in a]

from scipy import stats
a1 = stats.scoreatpercentile(target, per=100*OTHER_T, interpolation_method='fraction')
h2p.red_print("stats.scoreatpercentile:", a1)
a2 = stats.mstats.mquantiles(targetFP, prob=[OTHER_T])
h2p.red_print("scipy stats.mstats.mquantiles:", a2)

# looking at the sorted list here
targetFP.sort()
b = h2o_summ.percentileOnSortedList(targetFP, OTHER_T)
h2p.blue_print( "from scipy:", a2)

