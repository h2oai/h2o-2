import sys
sys.path.extend(['.','..','py'])
import h2o_print as h2p, h2o_summ
import numpy as np
import scipy as sp
import math
import argparse
OTHER_T = 0.25

# echo "FIX! have to update this to match current multipass exactQuantiles in Quantiles.java"

BIN_COUNT = 20
BIN_COUNT = 50
BIN_COUNT = 100000
BIN_COUNT = 2
BIN_COUNT = 1000
INTERPOLATION_TYPE=7

print "Using max_qbins: ", BIN_COUNT, "threshold:", OTHER_T

parser = argparse.ArgumentParser()
parser.add_argument('-nc', '--nocolor', help="don't emit the chars that cause color printing", action='store_true')
args = parser.parse_args()

# disable colors if we pipe this into a file to avoid extra chars
if args.nocolor:
    h2p.disable_colors()

# Defintion (this defn. seems odd. for the case of real quantiles, it should be a  floor, not a round up?)
# This definition may be correct for 1-based indexing. (we do zero-based indexing in the code below, so it looks different)

# For a finite population of N values indexed 1,...,N from lowest to highest, 
# the kth q-quantile of this population can be computed via the value of I_p = N * k/q. 
# "25th quantile":
# If I_p is not an integer, then round up to the next integer to get the appropriate index; the corresponding data value is the kth q-quantile. 
# If I_p is an integer then any number from the data value at that index to the data value of the next can be taken as the quantile, 
# and it is conventional (though arbitrary) to take the average of those two values (see Estimating the quantiles).
# "0.25 quantile"
# If, instead of using integers k and q, the "p-quantile" is based on a real number p with 0<p<1, then p 
# replaces k/q in the above formulae. Some software programs (including Microsoft Excel) regard the minimum and 
# maximum as the 0th and 100th percentile, respectively; however, such terminology is an extension beyond traditional statistics definitions.


# Multipass Binning implementatoin

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
# This does one (hcnt2) # histogram (could pass a list  of hcnt2s and associated state)

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
    newValBinSize  = newValRange / (desiredBinCnt + 0.0)
    newLowCount = 0 # count of rows below the bins
    # yes there is no newHighCount. Created during the pass, though.

    # state shared by each pass
    assert maxBinCnt > 0

    hcnt2 = [None for b in range(maxBinCnt)]
    hcnt2_min = [None for b in range(maxBinCnt)]
    hcnt2_max = [None for b in range(maxBinCnt)]
    hcnt2_low = 0
    hcnt2_high = 0

    assert newValBinSize != 0 # can be negative
    assert newValEnd > newValStart
    assert newValRange > 0

    # break out on stopping condition
    # reuse the histogram array hcnt2[]
    iteration = 0
    done = False
    # append to a list of best guesses per pass
    best_result = []

    def htot2():
        return sum(hcnt2) + hcnt2_low + hcnt2_high
        
    while iteration <= maxIterations and not done:
        h2p.green_print("newValStart", newValStart)
        h2p.green_print("newValEnd", newValEnd)
        h2p.green_print("newValRange", newValRange)
        h2p.green_print("newValBinSize", newValBinSize)
        h2p.green_print("newLowCount", newLowCount)
        h2p.green_print("threshold", threshold)

        valStart = newValStart
        valEnd   = newValEnd
        valRange = newValRange
        valBinSize = newValBinSize
        lowCount = newLowCount
        desiredBinCnt = BIN_COUNT
        maxBinCnt = desiredBinCnt + 1 # might go one over due to FP issues

        # playing with creating relative NUDGE values to make sure bin range
        # is always inclusive of target.
        # ratio it down from valBinSize. 
        # It doesn't need to be as big as valBinSize.
        # implicitly, it shouldn't need to be as large as valBinSize
        # can't seem to make it work yet. leave NUDGE=0
        NUDGE = 0

        # init to zero for each pass
        for b in range(maxBinCnt):
            hcnt2[b] = 0.0

        # Init counts outside of the bins
        hcnt2_low = 0
        hcnt2_high = 0

        # minimum value for higher than the bin. Needed for interpolation
        hcnt2_high_min = None

        for val in d:
            # Need to count the stuff outside the bin-gathering, 
            # since threshold compare is based on total row compare
            # on first pass, shouldn't see anything exceed the start/end bounds
            # since those are min/max for the column? (shouldn't be any fp precision issue? or ??)
            # oh wait, this valOffset math creates possible precision issue?
            # maybe we should address it with the NUDGE value below? but what about first pass?
            valOffset = val - valStart
            # where are we zeroing in? (start)
            binIdx2 = int(math.floor(valOffset / (valBinSize + 0.0))) # make sure it's always an fp divide?

            # do some close looking for possible fp arith issues
            cA = valOffset < 0
            cB = binIdx2 < 0
            t = {True: 1, False: 0}
            # we get the 10 case
            if ((cA and not cB) or (not cA and cB)):
                h2p.red_print("AB Interesting lower bin edge case %s%s" % (t[cA], t[cB]), "cA", cA, "cB", cB, "valOffSet", valOffSet, \
                    "binIdx2", binIdx2)
            cC = val > valEnd
            cD = binIdx2 >= (maxBinCnt-1) # tighten the compare for printing
            if ((cC and not cD) or (not cC and cD)):
                h2p.red_print("CD Interesting upper bin edge case %s%s" % (t[cC], t[cD]), "cC", cC, "cB", cD, "val", val, "valEnd", valEnd, \
                    "binIdx2", binIdx2, "maxBinCnt", maxBinCnt)
                # example hits this case..i.e. the max value
                # CD Interesting upper bin edge case 01 cC False cB True val 100.995097486 valEnd 100.995097486 binIdx2 2 maxBinCnt 3
                
            if valOffset < 0 or binIdx2<0:
            # if valOffset < 0:
            # if binIdx2<0:
                hcnt2_low += 1
            # prevent the extra bin from being used..i.e. eliminate the fuzziness for sure!
            # have to use both compares, since can wrap the index (due to start/end shift)
            # elif val > valEnd or binIdx2>=(maxBinCnt-1):
            # should this really be a valOffset compare?
            elif val > valEnd or binIdx2 >= maxBinCnt:
            # elif val > valEnd:
            # elif binIdx2>=(maxBinCnt-1):
                if (hcnt2_high==0) or (val < hcnt2_high_min):
                    hcnt2_high_min = val;
                    print "hcnt2_high_min update:", hcnt2_high_min, valOffset, val, valStart, hcnt2_high, val, valEnd
                hcnt2_high += 1
            else:
                # print "(multi) val: ",val," valOffset: ",valOffset," valBinSize: ",valBinSize

                assert binIdx2 >=0 and binIdx2<=(maxBinCnt-1), "val %s %s %s %s binIdx2: %s maxBinCnt: %s valBinSize: %s" % \
                    (val, valStart, valEnd, valOffset, binIdx2, maxBinCnt, valBinSize)
                if hcnt2[binIdx2]==0 or (val < hcnt2_min[binIdx2]):
                    hcnt2_min[binIdx2] = val;
                if hcnt2[binIdx2]==0 or (val > hcnt2_max[binIdx2]):
                    hcnt2_max[binIdx2] = val;
                hcnt2[binIdx2] += 1

                # check if we went into the magic extra bin
                if binIdx2 == (maxBinCnt-1):
                    print "\nFP! val went into the extra maxBinCnt bin:", \
                    binIdx2, hcnt2_high_min, valOffset, val, valStart, hcnt2_high, val, valEnd,"\n"
        
            # check the legal states for these two
            # we don't have None for checking hcnt2_high_min in java
            assert hcnt2_high==0 or (hcnt2_high_min is not None)
            assert (hcnt2_high_min is None) or hcnt2_high!=0

        # everything should either be in low, the bins, or high
        totalBinnedRows = htot2()
        print "totalRows check: %s htot2(): %s should be equal. hcnt2_low: %s hcnt2_high: %s" % \
            (totalRows, totalBinnedRows, hcnt2_low, hcnt2_high) 

        assert totalRows==totalBinnedRows, "totalRows: %s htot2() %s not equal. hcnt2_low: %s hcnt2_high: %s" % \
            (totalRows, totalBinnedRows, hcnt2_low, hcnt2_high) 

        # now walk thru and find out what bin to look inside
        currentCnt = hcnt2_low
        targetCntFull = threshold * (totalRows-1)  # zero based indexing
        targetCntInt = int(math.floor(threshold * (totalRows-1)))
        targetCntFract = targetCntFull  - targetCntInt
        assert targetCntFract>=0 and targetCntFract<=1
        print "targetCntInt:", targetCntInt, "targetCntFract", targetCntFract

        k = 0
        while ((currentCnt + hcnt2[k]) <= targetCntInt): 
            # print "looping for k (multi): ",k," ",currentCnt," ",targetCntInt," ",totalRows," ",hcnt2[k]," ",hcnt2_min[k]," ",hcnt2_max[k]
            currentCnt += hcnt2[k]
            # ugly but have to break out if we'd cycle along with == adding h0's until we go too far
            # are we supposed to advance to a none zero bin?
            k += 1 # goes over in the equal case?
            # if currentCnt >= targetCntInt:
            #     break
            if k==maxBinCnt:
                break
            assert k<maxBinCnt, "k too large, k: %s maxBinCnt %s %s %s %s" % (k, maxBinCnt, currentCnt, targetCntInt, hcnt2[k-1])

        # format string to match java Log.info() in Quantiles.java
        print "Found k (multi): ",k," ",currentCnt," ",targetCntInt," ",totalRows," ",hcnt2[k]," ",hcnt2_min[k]," ",hcnt2_max[k]
        assert hcnt2[k]!=1 or hcnt2_min[k]==hcnt2_max[k]

        # some possibily interpolating guesses first, in guess we have to iterate (best guess)
        done = False
        guess = (hcnt2_max[k] - hcnt2_min[k]) / 2

        # we maight not have gottent all the way
        if currentCnt==targetCntInt:
            if hcnt2[k]>2 and (hcnt2_min[k]==hcnt2_max[k]):
                guess = hcnt2_min[k]
                print "Guess A", guess, k, hcnt2[k]

            if hcnt2[k]==2:
                print "hello"
                print "\nTwo values in this bin but we could be aligned to the 2nd. so can't stop"
                # no mattter what size the fraction it would be on this number
                guess = (hcnt2_max[k] + hcnt2_min[k]) / 2.0
                # no mattter what size the fraction it would be on this number

                if INTERPOLATION_TYPE==2: # type 2 (mean)
                  guess = (hcnt2_max[k] + hcnt2_min[k]) / 2.0

                else: # default to type 7 (linear interpolation)
                  # Unlike mean, which just depends on two adjacent values, this adjustment  
                  # adds possible errors related to the arithmetic on the total # of rows.
                  dDiff = hcnt2_max[k] - hcnt2_min[k] # two adjacent..as if sorted!
                  pctDiff = targetCntFract # This is the fraction of total rows
                  guess = hcnt2_min[k] + (pctDiff * dDiff)

                done = False
                print "Guess B", guess

            if hcnt2[k]==1 and targetCntFract==0:
                assert hcnt2_min[k]==hcnt2_max[k]
                guess = hcnt2_min[k]
                done = True
                print "k", k
                print "Guess C", guess

            if hcnt2[k]==1 and targetCntFract!=0:
                assert hcnt2_min[k]==hcnt2_max[k]
                print "\nSingle value in this bin, but fractional means we need to interpolate to next non-zero"
                if k<maxBinCnt:
                    nextK = k + 1 # could put it over maxBinCnt
                else:
                    nextK = k
                while nextK<maxBinCnt and hcnt2[nextK]==0:
                    nextK += 1

                # have the "extra bin" for this
                if nextK >= maxBinCnt:
                    assert hcnt2_high!=0
                    print "Using hcnt2_high_min for interpolate:", hcnt2_high_min
                    nextVal = hcnt2_high_min
                else:
                    print "Using nextK for interpolate:", nextK
                    assert hcnt2[nextK]!=0
                    nextVal = hcnt2_min[nextK]

                guess = (hcnt2_max[k] + nextVal) / 2.0
                # OH! fixed bin as opposed to sort. Of course there are gaps between k and nextK

                if INTERPOLATION_TYPE==2: # type 2 (mean)
                    guess = (hcnt2_max[k] + nextVal) / 2.0
                    pctDiff = 0.5
                else: # default to type 7 (linear interpolation)
                    dDiff = nextVal - hcnt2_max[k] # two adjacent, as if sorted!
                    pctDiff = targetCntFract # This is the fraction of total rows
                    guess = hcnt2_max[k] + (pctDiff * dDiff)


                done = True # has to be one above us when needed. (or we're at end)

                print 'k', 'hcnt2_max[k]', 'nextVal'
                print "hello3:", k, hcnt2_max[k], nextVal
                print "\nInterpolating result using nextK: %s nextVal: %s" % (nextK, nextVal)
                print "Guess D", guess

        if not done:
            print "%s %s %s %s Not done, setting new range" % (hcnt2[k], currentCnt, targetCntInt, targetCntFract),\
                "k: ", k,\
                "currentCnt: ", currentCnt,\
                "hcnt2_min[k]: ", hcnt2_min[k],\
                "hcnt2_max[k]: ", hcnt2_max[k]

            # possible bin leakage at start/end edges due to fp arith.
            # the bin index arith may resolve OVER the boundary created by the compare for hcnt2_high compare
            # rather than using NUDGE, see if there's a non-zero bin below (min) or above (max) you.
            # Just need to check the one bin below and above k, if they exist. 
            if k > 0 and hcnt2[k-1]>0 and (hcnt2_max[k-1]<hcnt2_min[k]):
                print "1"
                newValStart = hcnt2_max[k-1]
            else:
                print "2"
                newValStart = hcnt2_min[k]

            # subtle. we do put stuff in the extra end bin (see the print above that happens)
            # k might be pointing to one less than that (like k=0 for 1 bin case)
            if k < maxBinCnt and hcnt2[k+1]>0 and (hcnt2_min[k+1]>hcnt2_max[k]):
                print "3"
                newValEnd = hcnt2_min[k+1]
            else:
                print "4"
                newValEnd = hcnt2_max[k]
            
            newValRange = newValEnd - newValStart 
            # maxBinCnt is always binCount + 1, since we might cover over due to rounding/fp issues?
            newValBinSize = newValRange / (desiredBinCnt + 0.0)
            
            # the start/end should never change if we're just using one bin
            # this is a bin leakage test, if you use one bin. (we should never resolve exactly stop at max iterations
            # assumes NUDGE is 0
            if NUDGE == 0.0:
                assert desiredBinCnt>1 or (valStart==newValStart and valEnd==newValEnd),\
                    "if 1 bin, should be no per-pass edge leakage %s %s %s %s %s %s" % (k, hcnt2_high, valStart, newValStart, valEnd, newValEnd)
            newLowCount = currentCnt
            if newValBinSize==0:
                # assert done or newValBinSize!=0 and live with current guess
                print "Assuming done because newValBinSize is 0."
                print "newValRange: %s, hcnt2[k]: %s hcnt2_min[k]: %s hcnt2_max[k]: %s" %\
                     (newValRange, hcnt2[k], hcnt2_min[k], hcnt2_max[k])
                guess = newValStart
                print "Guess E", guess
                # was done = True 3/20/14
                done = True

            # if we have to interpolate
            # if it falls into this bin, interpolate to this bin means one answer?

            # cover the case above with multiple entries in a bin, all the same value
            # will be zero on the last pass?
            # assert newValBinSize != 0 or done
            # need the count up to but not including newValStart

        best_result.append(guess)
        iteration += 1

        h2p.blue_print("Ending Pass", iteration)
        h2p.blue_print("best_result:", best_result, "done:", done, "hcnt2[k]", hcnt2[k])
        print "currentCnt", currentCnt, "targetCntInt", targetCntInt, "hcnt2_low", hcnt2_low, "hcnt2_high", hcnt2_high
        print "was", valStart, valEnd, valRange, valBinSize
        print "next", newValStart, newValEnd, newValRange, newValBinSize

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
csvPathname = './syn_binary_100000x1.csv'
csvPathname = './d.csv'
csvPathname = './runif_.csv'
csvPathname = './covtype1.data'
csvPathname = './runif_.csv'
csvPathname = '/home/0xdiag/datasets/kmeans_big/syn_sphere_gen.csv'
csvPathname = './syn_binary_100000x1.csv'
csvPathname = '/home/kevin/h2o/smalldata/quantiles/breadth.csv'
csvPathname = './wonky1'
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
print dataset.shape
col = 0
if len(dataset.shape)==1:
    target = dataset
else:
    target = [x[col] for x in dataset]

targetFP = np.array(target, np.float)

n_features = len(dataset) - 1
print "n_features:", n_features

print "histogram of target"
# print target
print sp.histogram(target)

thresholds   = [0.001, 0.01, 0.1, 0.25, 0.33, 0.5, 0.66, 0.75, 0.9, 0.99, 0.999]

#*****************************************************************
# h2o
#*****************************************************************
d = target


    # target = dataset

dmin = min(d)
dmax = max(d)
thresholdList = [OTHER_T]

quantiles = findQuantileList(d, dmin, dmax, thresholdList)
h2p.red_print('\nthis b result:', quantiles)
#*****************************************************************
# for comparison
#*****************************************************************
# perPrint = ["%.2f" % v for v in a]
# scipy apparently doesn't have the use of means (type 2)
# http://en.wikipedia.org/wiki/Quantile
# it has median (R-8) with 1/3, 1/3

# type 6
alphap=0
betap=0

# type 5 okay but not perfect
alphap=0.5
betap=0.5

# type 8
alphap=1/3.0
betap=1/3.0



# an approx? (was good when comparing to h2o type 2)
alphap=0.4
betap=0.4

# this is type 7
alphap=1
betap=1


from scipy import stats
a1 = stats.scoreatpercentile(target, per=100*OTHER_T, interpolation_method='fraction')
h2p.red_print("stats.scoreatpercentile:", a1)
a2 = stats.mstats.mquantiles(targetFP, prob=[OTHER_T], alphap=alphap, betap=betap)
h2p.red_print("scipy stats.mstats.mquantiles:", a2)
targetFP.sort()
b = h2o_summ.percentileOnSortedList(targetFP, OTHER_T, interpolate='linear')
h2p.red_print("sort algo:", b)
h2p.red_print( "from h2o (multi):", quantiles[0])

print "Now looking at the sorted list..same thing"
h2p.blue_print("stats.scoreatpercentile:", a1)
a2 = stats.mstats.mquantiles(targetFP, prob=[OTHER_T], alphap=alphap, betap=betap)
h2p.blue_print("scipy stats.mstats.mquantiles:", a2)
b = h2o_summ.percentileOnSortedList(targetFP, OTHER_T, interpolate='linear')
h2p.blue_print("sort algo:", b)
h2p.blue_print( "from h2o (multi):", quantiles[0])

