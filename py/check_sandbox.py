#!/usr/bin/python
import sys
sys.path.extend(['.','..','py'])
import h2o_sandbox
print "Will look at all the files in ./sandbox assuming they are stdout/stderr log files"
h2o_sandbox.check_sandbox_for_errors(pattern='*')

