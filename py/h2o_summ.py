


# Courtesy of Wai Yip Tung. a pure python percentile function
# so we don't have to use the one(s) from numpy or scipy
# and require those package installs
## {{{ http://code.activestate.com/recipes/511478/ (r1)

import math
import functools

def percentileOnSortedList(N, percent, key=lambda x:x):
    """
    Find the percentile of a list of values.

    @parameter N - is a list of values. Note N MUST BE already sorted.
    @parameter percent - a float value from 0.0 to 1.0.
    @parameter key - optional key function to compute value from each element of N.

    @return - the percentile of the values
    """
    if not N:
        return None
    k = (len(N)-1) * percent
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return key(N[int(k)])
    d0 = key(N[int(f)]) * (c-k)
    d1 = key(N[int(c)]) * (k-f)
    return d0+d1

# median is 50th percentile.
def medianOnSortedList(N, key=lambda x:x):
    median = functools.partial(percentileOnSortedlist, percent=0.5)
    return median

def percentileOnSortedList_25_50_75( N, key=lambda x:x):
    three = (
        functools.partial(percentileOnSortedlist, percent=0.25)
        functools.partial(percentileOnSortedlist, percent=0.50)
        functools.partial(percentileOnSortedlist, percent=0.75)
    )
    return three
