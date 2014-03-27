package hex.deeplearning;

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
  public byte[] bits() { return _bits; }

  public Dropout() {
  }

  @Override
  public String toString() {
    String s = "Dropout: " + super.toString();
    s += "\nRandom: " + _rand.toString();
    s += "\nbits: ";
    for (int i=0; i< _bits.length*8; ++i) s += unit_active(i) ? "1":"0";
    s += "\n";
    return s;
  }

  Dropout(int units) {
    _bits = new byte[(units+7)/8];
    _rand = new Random(0);
  }

  // for input layer
  public void randomlySparsifyActivation(double[] a, double rate, long seed) {
    if (rate == 0) return;
    setSeed(seed);
    for( int i = 0; i < a.length; i++ )
      if (_rand.nextFloat() < rate) a[i] = 0;
  }

  // for hidden layers
  public void fillBytes(long seed) {
    setSeed(seed);
    _rand.nextBytes(_bits);
  }

  public boolean unit_active(int o) {
    return (_bits[o / 8] & (1 << (o % 8))) != 0;
  }

  private void setSeed(long seed) {
    if ((seed >>> 32) < 0x0000ffffL)         seed |= 0x5b93000000000000L;
    if (((seed << 32) >>> 32) < 0x0000ffffL) seed |= 0xdb910000L;
    _rand.setSeed(seed);
  }

  @Test
  public void test() throws Exception {
    final int units = 1000;
    double[] a = new double[units];
    double sum1=0, sum2=0, sum3=0, sum4=0;

    final int loops = 10000;
    for (int l = 0; l < loops; ++l) {
      Dropout d = new Dropout(units);
      long seed = new Random().nextLong();
      Arrays.fill(a, 1.);
      d.randomlySparsifyActivation(a, 0.3, seed);
      sum1 += water.util.Utils.sum(a);
      Arrays.fill(a, 1.);
      d.randomlySparsifyActivation(a, 0.0, seed+1);
      sum2 += water.util.Utils.sum(a);
      Arrays.fill(a, 1.);
      d.randomlySparsifyActivation(a, 1.0, seed+2);
      sum3 += water.util.Utils.sum(a);
      d.fillBytes(seed+3);
      for (int i=0; i<units; ++i)
        if (d.unit_active(i)) sum4++;
//      Log.info(d.toString());
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
