#!/bin/bash
# beforeCommit.sh -- created 2012-10-11, <+NAME+>
# @Last Change: 24-Dez-2004.
# @Revision:    0.0

nosetests test.py
nosetests test_inspect.py
nosetests test_putfile.py

# vi: 
