sys.path.extend(['.','..','py'])
import h2o_print as h2p, h2o_summ
import numpy as np
import scipy as sp
import sys
import math
OTHER_T = 0.50
BIN_COUNT = 1000

# recursive binning to a single bin with one number
# might have multiple rows with that one number
# possible answers:
#   that number if single row  in that bin, 
#   that number if the threshold is fractional and 2 or more rows in that bin
#   if 1 row in the last bin, have to interpolate (mean), to the next sorted value 
#   this will be the min of the "outside the bin" (min of everthing to the right of the last bin)
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

# Can process multiple histograms with different binning goals, per data pass. This does one (hcnt2)
# histogram (could pass a list  of hcnts and associated state)

# each pass creates a best guess from the info available at that pass
# best_result[] list then shows the incremental improvement per pass 

#****************************************************************************
def findQuantile(d, dmin, dmax, drows, threshold):
    # return the value at the threshold, or the mean of the two rows that bound it.
    # fixed bin count per pass. Stops at maxIterations if not resolved to one true answer
    maxIterations = 30

    # totalRows should be cleansed of NAs. assume d doesn't have NAs (cleaned elsewhere)
    totalRows = len(drows)

    # initialize
    newValStart = dmin
    newValEnd   = dmax
    newValRange = newValEnd - newValStart
    desiredBinCnt = BIN_COUNT # Could do per-pass adjustment, but fixed works fine.
    newBinSize  = newValRange / (desiredBinCnt + 0.0)
    newLowCount = 0 # count of rows below the bins
    # yes there is no newHighCount. Created during the pass, though.

    # state shared by each pass
    # Used to have 
    hcnt = [None for b in range(maxBinCnt)]
    hcnt_min = [None for b in range(maxBinCnt)]
    hcnt_max = [None for b in range(maxBinCnt)]
    hcnt_low = 0
    hcnt_high = 0

    assert newBinSize != 0 # can be negative
    assert NewValEnd > newValstart
    assert NewValRange > 0
    assert NewBinCnt > 0

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
        desiredBinCnt = BIN_COUNT
        lowCount = newLowCount

        maxBinCnt = desiredBinCount + 1 # might go one over due to FP issues

        # playing with creating relative NUDGE values to make sure bin range
        # is always inclusive of target.
        NUDGE = 1e-3
        NUDGE = (1000 * (valEnd - valStart)) / 1000000
        # ratio it down from binSize. 
        # It doesn't need to be as big as binSize.
        # implicitly, it shouldn't need to be as large as binSize
        NUDGE = binSize / desiredBinCount
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
                    print "First addition to hcnt_high this pass, val:", val, "valEnd:", valEnd
                    hcnt_high_min = val
                else:
                    hcnt_high_min = min(hcnt_high_min, val)
                hcnt_high += 1
            else:
                # where are we zeroing in? (start)
                # print valOffset, binSize
                hcntIdx = int(round((valOffset * 1000000.0) / binSize) / 1000000.0)
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
        k = 0
        currentCnt = newLowCount
        targetCntInt = int(math.floor(threshold * totalRows))
        targetCntFract = (threshold * totalRows) - targetCntInt
        exactRowPossible = targetCntFract==0.0

        print "targetCntInt:", targetCntInt, "targetCntFract", targetCntFract

        while((currentCnt + hcnt[k]) <= targetCntInt): 
            currentCnt += hcnt[k]
            k += 1
            assert k <= maxBinCnt, "k too large, k: %s maxBinCnt %s" % (k, maxBinCnt)

        # I guess we don't care about the values at the bin edge
        # binLeftEdge = valStart + k*binSize
        # binRightEdge = valStart + k*binSize
        # this might fail if there are fp issues, but will show we might need smudging on the bin boundaries or ??
        if currentCnt==targetCntInt or hcnt[k]==0:
            if hcnt[k]!=0:
                guess = hcnt_min[k]
                h2p.red_print ("Guess A", guess, currentCnt, targetCntInt)
            else:
                if k==0:
                    assert hcnt[k+1]!=0  # "Unexpected state of starting hcnt bins"
                    guess = hcnt_min[k+1] # use the first value in the next bin
                    h2p.red_print ("Guess B", guess, currentCnt, targetCntInt)
                else:
                    if hcnt[k-1]!=0:
                      guess = hcnt_max[k-1] # use the last value in the prior bin
                      h2p.red_print ("Guess C", guess, currentCnt, targetCntInt)
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
            guess = hcnt_min[k] + (actualBinWidth * ((targetCntInt - currentCnt)/ hcnt[k]))
            print "Guess D:", 'guess', 'k', 'hcnt_min[k]', 'actualBinWidth', 'currentCnt', 'targetCntInt', 'hcnt[k]'
            print "Guess D:", guess, k, hcnt_min[k], actualBinWidth, currentCnt, targetCntInt, hcnt[k]

        # We should end with a count of 1, otherwise it's still a best guess
        # could be approximately equal
        # THERE CAN BE MULTIPLE VALUES AT THE TARGET VALUE
        # chenk for min = max in that bin!
        print "checking for done, hcnt_min[k]", hcnt_min[k], "hcnt_max[k]", hcnt_max[k]

        # In the right bit with only one value, and the 
        if hcnt_min[k]==hcnt_max[k] and currentCnt==targetCntInt: 
            # no mattter what size the fraction it would be on this number
            if hcnt[k]>=2 and targetCntIntFraction==0:
                done = True
                guess = hcnt_min[k]

        if done:
            print 'Done:', 'hcnt_min[k]', 'hcnt_max[k]', 'currentCnt', 'targetCntInt', 'targetCntFract'
            print 'Done:', hcnt_min[k], hcnt_max[k], currentCnt, targetCntInt, targetCntFract
        else:
            print 'Not Done:', 'hcnt_min[k]', 'hcnt_max[k]', 'currentCnt', 'targetCntInt', 'targetCntFract'
            print 'Not Done:', hcnt_min[k], hcnt_max[k], currentCnt, targetCntInt, targetCntFract

        # do we have to compute the mean, using the current k and nextK bins?
        # if min and max for a bin are different the count must be >1
        if hcnt[k]==1: 
            assert hcnt_min[k]==hcnt_max[k]

        # need to get to 1 entry to 
        # if there's a fractional part, and we're not done, it's in the next k

        # okay...potentially multiple of same value in the bin
        if not done and hcnt[k]>1 and hcnt_min[k]==hcnt_max[k] and targetCntFract!=0:
            print "\nInterpolating result into single value of this bin"
            print 'Guess E:', 'guess', 'k', 'hcnt[k]', 'hcnt_min[k]', 'hcnt_max[k]', 'currentCnt', 'targetCntInt'
            print "Guess E:", guess, k, hcnt[k], hcnt_min[k], hcnt_max[k], currentCnt, targetCntInt
            guess = hcnt_min[k]
            done = True

        if not done and hcnt[k]==1 and hcnt_min[k]==hcnt_max[k] and targetCntFract!=0:
            print "\nSingle value in this bin, but fractional means we need to interpolate to next non-zero"
            print 'k', 'hcnt[k]', 'hcnt_min[k]', 'hcnt_max[k]', 'currentCnt', 'targetCntInt'
            print k, hcnt[k], hcnt_min[k], hcnt_max[k], currentCnt, targetCntInt

        # one in bin. look to next bin
        if not done and hcnt[k]==1 and targetCntFract!=0:
            # only legitimate case is !exactRowPossible
            assert not exactRowPossible
            print "Trying to find nextK for possibly interpolating k: %s" % k
            # always figure nextK
            # should we take the mean of the current and next non-zero bin
            # find the next non-zero bin too
            if k<maxBinCnt:
                nextK = k + 1 # could put it over maxBinCnt
            else:
                nextK = k
            
            while nextK<maxBinCnt and hcnt[nextK]==0:
                nextK += 1

            if nextK>=maxBinCnt:
                nextK = None
                print "nextK is outside the bins. Use hcnt_high_min"
                if hcnt_high > 0:
                    print "\nInterpolating result using hcnt_high_min"
                    print "Guess F with hcnt_high_min:"
                    print 'guess', 'k', 'hcnt_high_min', ' currentCnt', 'targetCntInt', 'targetCntFract', 'hcnt[k]'
                    print guess, k, hcnt_high_min,  currentCnt, targetCntInt, targetCntFract, hcnt[k]
                    guess = (hcnt_max[k]+ hcnt_high_min) / 2.0
                else:
                    print "Guess F with hcnt_high_min not valid:", \
                        guess, k, hcnt_high, hcnt_high_min,  currentCnt, targetCntInt, targetCntFract, hcnt[k]
                    assert false # should never happen?
                    guess = hcnt_high

            else:
                # last might be empty
                assert nextK<maxBinCnt, "%s %s" % (nextK, maxBinCnt)
                if hcnt[nextK]==0:
                    nextK = k

                nextCnt = int(nextK * binSize)

                # have the "extra bin" for this
                assert nextK < (maxBinCnt+1), "nextK too large, nextK: %s maxBinCnt %s" % (nextK, maxBinCnt)
                    
                print "k:", k, "nextK", nextK    
                if  k != nextK:
                    guess = (hcnt_max[k] + hcnt_min[nextK]) / 2.0
                    print "\nInterpolating result using nextK"
                    print "Guess G with nextK:", guess, k, nextK,  hcnt_max[k], hcnt_min[nextK], currentCnt, targetCntInt, hcnt[k]

                # since we moved into the partial bin
                assert hcnt[nextK]!=0, hcnt[nextK]
                assert hcnt[k]!=0, hcnt[k]

            # now we're done

        newValStart = hcnt_min[k] - NUDGE# FIX! should we nudge a little?
        newValEnd   = hcnt_max[k] + NUDGE # FIX! should we nudge a little?
        newValRange = newValEnd - newValStart 
        
        # maxBinCnt is always binCount + 1, since we might cover over due to rounding/fp issues?
        newBinSize = newValRange / (desiredBinCnt + 0.0)
        newMaxBinCnt = desiredBinCnt + 1

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
        print "currentCnt", currentCnt, "targetCntInt", targetCntInt, "hcnt_low", hcnt_low, "hcnt_high", hcnt_high
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

