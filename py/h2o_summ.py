# similar to Wai Yip Tung. a pure python percentile function
# so we don't have to use the one(s) from numpy or scipy
# and require those package installs
## {{{ http://code.activestate.com/recipes/511478/ (r1)

import math
import functools

def percentileOnSortedList(N, percent, key=lambda x:x, interpolate='linear'):
    # 5 ways of resolving fractional
    # floor, ceil, funky, linear, mean
    interpolateChoices = ['floor', 'ceil', 'funky', 'linear', 'mean']
    if interpolate not in interpolateChoices:
        print "Bad choice for interpolate:", interpolate
        print "Supported choices:", interpolateChoices
    """
    Find the percentile of a list of values.

    @parameter N - is a list of values. Note N MUST BE already sorted.
    @parameter percent - a float value from 0.0 to 1.0.
    @parameter key - optional key function to compute value from each element of N.

    @return - the percentile of the values
    """
    if N is None:
        return None
    k = (len(N)-1) * percent

    f = int(math.floor(k))
    c = int(math.ceil(k))
    if f == c:
        d = key(N[k])
        msg = "aligned:" 

    elif interpolate=='floor':
        d = key(N[f])
        msg = "fractional with floor:" 

    elif interpolate=='ceil':
        d = key(N[c])
        msg = "fractional with ceil:" 

    elif interpolate=='funky':
        d0 = key(N[f]) * (c-k)
        d1 = key(N[c]) * (k-f)
        d = d0+d1
        msg = "fractional with Tung(floor and ceil) :" 
    
    elif interpolate=='linear':
        pctDiff = (k-f)/(c-f+0.0)
        dDiff = pctDiff * (key(N[c]) - key(N[f]))
        d = key(N[f] + dDiff)
        msg = "fractional %s with linear(floor and ceil):" % pctDiff

    elif interpolate=='mean':
        d = (key(N[c]) + key(N[f])) / 2.0
        msg = "fractional with mean(floor and ceil):" 

    # print 3 around the floored k, for eyeballing when we're close
    flooredK = int(f)
    # print the 3 around the median
    if flooredK > 0:
        print "prior->", key(N[flooredK-1]), " "
    else:
        print "prior->", "<bof>"
    print "floor->", key(N[flooredK]), " ", msg, 'result:', d
    if flooredK+1 < len(N):
        print " ceil->", key(N[flooredK+1])
    else:
        print " ceil-> <eof>"

    return d

# median is 50th percentile.
def medianOnSortedList(N, key=lambda x:x):
    median = percentileOnSortedlist(N, percent=0.5, key=key)
    return median

def percentileOnSortedList_25_50_75( N, key=lambda x:x):
    three = (
        percentileOnSortedlist(N, percent=0.25, key=key),
        percentileOnSortedlist(N, percent=0.50, key=key),
        percentileOnSortedlist(N, percent=0.75, key=key),
    )
    return three
