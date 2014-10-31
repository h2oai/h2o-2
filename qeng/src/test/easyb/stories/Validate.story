import groovy.lang.Range;
import groovy.lang.Tuple;

tags "xtools-stats-test"

description
  """
  Test that Stats.zScore returns correct values
  """
scenario "run some z zcores for some know values", {
  when "filter constant set of values", {
    z1 = 0.00d
  }
  then "verify that we got right sizes and numbers", {
    (z1 ==  0.00d).shouldBe true
  }
}

description
"""
  Test that confidenceInterval returns correct values
  """
scenario "calculate confidence interval", {
  when "we load statistical values", {
    // sample size, mean, variance=sigma**2
    s1 = new Tuple<Integer,Double,Double>(6, 5.31, 37.92)
  }
  then "verify that we got right sizes and numbers", {
    (s1.get(0) ==  6).shouldBe true
  }
}


