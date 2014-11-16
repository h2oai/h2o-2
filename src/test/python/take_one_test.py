import unittest
from proboscis.asserts import assert_equal
from proboscis.asserts import assert_false
from proboscis.asserts import assert_raises
from proboscis.asserts import assert_true
from proboscis import after_class
from proboscis import before_class
from proboscis import SkipTest
from proboscis import test

import random
import types
import unittest
import sys, pprint
sys.path.extend(['.','..','py'])

@test(groups=["acceptance"])
class TestModelManagement(object):
    @test(groups=['acceptance'])
    def testA(self):
        self.a = "a"
        assert_equal(self.a, "a")

    @test(groups=["acceptance"], depends_on=[testA])
    def testB(self):
        self.b = "b"
        assert_equal ( self.b, "b" )

    @test(groups=["acceptance"], depends_on=[testB])
    def testC(self):
        self.c = "c"
        assert_equal ( self.c, "c" )


## ----------------- proboscis boiler plate hook -------------------------
# no reason to modify anything below
def run_tests():
    from proboscis import TestProgram
    TestProgram().run_and_exit()

if __name__ =='__main__':
    run_tests()
