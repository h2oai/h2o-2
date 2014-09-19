#!/usr/bin/python
# http://axialcorps.com/2013/09/27/dont-slurp-how-to-read-files-in-python/
# a simple filter that prepends line numbers
import sys
# this reads in one line at a time from stdin
s = 0.0
count = 0
for line in sys.stdin:
    f = float(line)
    s += f
    count += 1
    # print "%.13f %.13f" % (f, s)

print "%.13f %.13f" % (f, s)
print "sum:", "%.13f" % s
print "count:", count
