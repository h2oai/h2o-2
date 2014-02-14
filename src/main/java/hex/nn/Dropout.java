package hex.nn;

import junit.framework.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Random;

/**
 * Helper class for dropout training of Neural Nets
 */
public class Dropout {
  private transient Random _rand;
  private transient byte[] _bits;

  public Dropout() {
  }

  Dropout(int units) {
    _bits = new byte[(units+7)/8];
    _rand = new Random();
  }

  void setSeed(long seed) {
    if ((seed >>> 32) < 0x0000ffffL)         seed |= 0x5b93000000000000L;
    if (((seed << 32) >>> 32) < 0x0000ffffL) seed |= 0xdb910000L;
    _rand.setSeed(seed);
  }

  // for input layer
  public void randomlySparsifyActivation(double[] a, double rate) {
    Assert.assertTrue("Must call setSeed() first", _rand != null);
    for( int i = 0; i < a.length; i++ )
      if (_rand.nextFloat() < rate) a[i] = 0;
  }

  // for hidden layers
  public void fillBytes() {
    Assert.assertTrue("Must call setSeed() first", _rand != null);
    _rand.nextBytes(_bits);
  }

  public boolean unit_active(int o) {
    return (_bits[o / 8] & (1 << (o % 8))) != 0;
  }

  @Test
  public void test() throws Exception {
    final int units = 1000;
    double[] a = new double[units];
    double sum1=0, sum2=0, sum3=0, sum4=0;

    final int loops = 10000;
    for (int l = 0; l < loops; ++l) {
      Dropout d = new Dropout(units);
      d.setSeed(new Random().nextLong());
      Arrays.fill(a, 1.);
      d.randomlySparsifyActivation(a, 0.3);
      sum1 += water.util.Utils.sum(a);
      Arrays.fill(a, 1.);
      d.randomlySparsifyActivation(a, 0.0);
      sum2 += water.util.Utils.sum(a);
      Arrays.fill(a, 1.);
      d.randomlySparsifyActivation(a, 1.0);
      sum3 += water.util.Utils.sum(a);
      d.fillBytes();
      for (int i=0; i<units; ++i)
        if (d.unit_active(i)) sum4++;
    }
    sum1 /= loops;
    sum2 /= loops;
    sum3 /= loops;
    sum4 /= loops;
    Assert.assertTrue(Math.abs(sum1-700)<1);
    Assert.assertTrue(sum2 == units);
    Assert.assertTrue(sum3 == 0);
    Assert.assertTrue(Math.abs(sum4-500)<1);
  }
}
