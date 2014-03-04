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
col = 0

dataset = np.genfromtxt(
    open(csvPathname, 'r'),
    delimiter=',',
    skip_header=1,
    dtype=None); # guess!

print dataset.shape
# target = [x[col] for x in dataset]
# one column
target = dataset
targetFP = np.array(target, np.float)

n_features = len(dataset) - 1;
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
totalRows = len(d)

# fixed bin count per pass
binCount = 5
dmin = min(d)
dmax = max(d)

threshold = 0.99

newValStart = dmin
newValEnd   = dmax
newValRange = newvalEnd - newValStart
newBinCount = binCount
newBinSize = nvewValRange / (newBinCount + 0)

# check if val is NaN and ignore?
targetCount = math.floor(threshold * totalRows)

# break out on stopping condition
# reuse the histogram array hcnt[]
for passId in range(0, 5):
    valStart = newValStart
    valEnd   = newValEnd
    valWidth = newValWidth
    binSize = newBinSize

    for b in range(1, binsCount):
        binLeftEdge[b] = binStart + b*binSize
        hCnt[b] = 0

    for val in d:
        # where are we zeroing in (start)
        valOffset = val - ValBase[passId]
        hcntIdx = round(((valOffset - ) * 1000000.0) / binsz) / 1000000;

        assert hcntIdx >=0 and hcntIdx<=10000

        if len(hcnt) < hcntIdex):
            hcnt.append(0.0)
            hcnt_min.append(None)
            hcnt_max.append(None)

        hcnt[hcntIdx] += 1
        if hcnt[hcntIdx]==0:
            hcnt_min[hcntIdx] = val
            hcnt_max[hcntIdx] = val
        else:
            hcnt_min[hcntIdx] = min(hcnt_min[hcntIdx], val)
            hcnt_max[hcntIdx] = max(hcnt_max[hcntIdx], val)

    # now walk thru and find out what bin you look at it's valOffset (which will be the hcnt_min for that bin
    s = 0
    k = 0
    s1 = int(math.floor(threshold * drows))
    while( (s+hcnt2[k]) < s1) { 
        s += hcnt2[k];
        k++;
        assert k < binCount[passId]
    }

    # adjust the start to be that value
    # subtract something to cover fp error?
    # keep one bin below that 
        newValStart = hcnt_min[k]
        newValEnd   = hcnt_max[k]
        newValWidth = NewEnd - newStart
        newBinSize = newValWidth / (binCount[passId+1])





    



