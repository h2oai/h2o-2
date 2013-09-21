package hex;

import static org.junit.Assert.*;

import java.util.*;

import org.junit.Test;

import water.*;
import water.fvec.*;


public class QuantileTest extends TestUtil {

  /* Custom boilerplate around JUnit testrunner.
   * Note: Do not try to make or run inside Eclipse as a JUnit test, that won't work.
   *   JUnit tests like this must instead be run as a regular Java application ("water.Boot")
   *   using Earl's custom JUnit testrunner below.
   */
  public static class BootH2O {
    public static void main(String[] args) throws Exception{
      water.Boot.main(QuantileTest.class, args);
    }
  }
  public static void main(String[] args){
    H2O.main(args);
    try {
      QuantileTest qtest = new QuantileTest();
      qtest.testGaussianDoubleSummary_small();
    } catch(Throwable e){
      e.printStackTrace();
    } finally {
      UDPRebooted.suicide(UDPRebooted.T.shutdown, H2O.SELF);
    }
  }

  // Actual JUnit tests go here

  @Test public void testGaussianDoubleSummary_small() {
    testGaussianDoubleSummary(512000);
  }

  /*@Test public void testGaussianDoubleSummary_big() {
    testGaussianDoubleSummary((long)1e8);
  }*/

  public void testGaussianDoubleSummary(long n) {

    // Setup: create a Vec of size n, populate with normal dist over a wide variance
    Vec d = makeRandomDoubleVec(n, 0.1, /*Math.pow(10., Double.MAX_EXPONENT/4.),*/ 1234567890001L);
    System.out.println("DEBUG: " + d + " length:" + d.length()); // + d.at()
    for (long i=0; i<d.length(); i+= 1000) {
      System.out.println("  d[" + i +"]=" + d.at(i));
    }

    // Test for typical quantiles...
    double[] quantilesTypical = new double[] {0.05, 0.1, 0.15, 0.85, 0.9, 0.95};
    Quantiles qq = new Quantiles(d, quantilesTypical);
    System.out.println("Quantiles: " + Arrays.toString(quantilesTypical));
    System.out.println("Results: " + Arrays.toString(qq.qval));
    // The result values below are for n=512000
    assertEquals(qq.qval[0], -1.2591825e13, 1.);
    assertEquals(qq.qval[1], -4.026675e8, 1.);
    assertEquals(qq.qval[2], -223163.5, 1.);
    assertEquals(qq.qval[3], +221262.5, 1.);
    assertEquals(qq.qval[4], +3.943455e8, 1.);
    assertEquals(qq.qval[5], +1.2889925e13, 1.);

    // Test for extreme quantiles...
    double[] quantilesExtreme = new double[] {0.005, 0.01, 0.02, 0.98, 0.99, 0.995};
    qq = new Quantiles(d, quantilesExtreme);
    System.out.println("Quantiles: " + Arrays.toString(quantilesExtreme));
    System.out.println("Results: " + Arrays.toString(qq.qval) + "\n");
    assertEquals(qq.qval[0], -7.085399999999542e23, 1.);
    assertEquals(qq.qval[1], -9.8906e20, 1.);
    assertEquals(qq.qval[2], -8.371195e17, 1.);
    assertEquals(qq.qval[3], +8.2954649999999987e17, 1.);
    assertEquals(qq.qval[4], +1.1076249999998589e21, 1.);
    assertEquals(qq.qval[5], +7.438600000000224e23, 1.);
  }

  private Vec makeRandomDoubleVec(long n, double mean, /*double variance,*/ long seed) {

    RandomGaussianExponential RNG = new RandomGaussianExponential(mean, /*variance,*/ seed);

    AppendableVec vec = new AppendableVec(UUID.randomUUID().toString());
    NewChunk nc = new NewChunk(vec,0/*starting chunk#*/);
    for (long i=0; i<n; i++)
        nc.addNum(RNG.next());
    nc.close(0/*actual chunk number*/, null);

    Vec v = vec.close(null); // should annotate "throws OutOfMemoryException..." to JUnit
    DKV.put(Key.make("random doubles"), v);
    return v;
  }

  /**
   * Generate pseudo-random doubles with approximately Gaussian (normal) distribution.
   *
   * Prefer this to org.apache.commons.math3.distribution.NormalDistribution to avoid
   * bloating jarfile.
   *
   * Adapted from recipe: http://www.javapractices.com/topic/TopicAction.do?Id=62
   */
  static public final class RandomGaussianExponential {

    private double mean;
    //private double variance;
    private Random RNG;

    public RandomGaussianExponential(double mean, /*double variance,*/ long seed) {
      this.mean = mean;
      //this.variance = variance;
      this.RNG = new Random(seed);
    }
    public RandomGaussianExponential() {
      this(0.0, /*Math.pow(10.,Double.MAX_EXPONENT),*/ 1234567890001L);
    }

    private double next() {
      /**
       *  To get a Gaussian distribution across all exponents, we first generate
       * a random exponent, then exponentiate... */
      double randomExponent = Double.MAX_EXPONENT/100. * RNG.nextGaussian();
      return this.mean + randomSign() * Math.pow(10., randomExponent);

      /* ...whereas a plain Gaussian distribution would simply use:
       *  this.mean + RNG.nextGaussian() * this.variance
       */
    }
    private double randomSign() { return RNG.nextBoolean() ? +1 : -1; }
  }

}


