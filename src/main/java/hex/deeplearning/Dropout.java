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
  private transient double _rate;

  public double rate() { return _rate; }
  public byte[] bits() { return _bits; }

  public Dropout() {
    _rate = 0.5;
  }

  @Override
  public String toString() {
    String s = "Dropout: " + super.toString();
    s += "\nRandom: " + _rand.toString();
    s += "\nDropout rate: " + _rate;
    s += "\nbits: ";
    for (int i=0; i< _bits.length*8; ++i) s += unit_active(i) ? "1":"0";
    s += "\n";
    return s;
  }

  Dropout(int units) {
    _bits = new byte[(units+7)/8];
    _rand = new Random(0);
    _rate = 0.5;
  }

  Dropout(int units, double rate) {
    this(units);
    _rate = rate;
  }

  // for input layer
  public void randomlySparsifyActivation(float[] a, long seed) {
    if (_rate == 0) return;
    setSeed(seed);
    for( int i = 0; i < a.length; i++ )
      if (_rand.nextFloat() < _rate) a[i] = 0;
  }

  // for hidden layers
  public void fillBytes(long seed) {
    setSeed(seed);
    if (_rate == 0.5) _rand.nextBytes(_bits);
    else {
      Arrays.fill(_bits, (byte)0);
      for (int i=0;i<_bits.length*8;++i)
        if (_rand.nextFloat() > _rate) _bits[i / 8] |= 1 << (i % 8);
    }
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
    float[] a = new float[units];
    double sum1=0, sum2=0, sum3=0, sum4=0;

    final int loops = 10000;
    for (int l = 0; l < loops; ++l) {
      long seed = new Random().nextLong();

      Dropout d = new Dropout(units, 0.3);
      Arrays.fill(a, 1f);
      d.randomlySparsifyActivation(a, seed);
      sum1 += water.util.Utils.sum(a);

      d = new Dropout(units, 0.0);
      Arrays.fill(a, 1f);
      d.randomlySparsifyActivation(a, seed + 1);
      sum2 += water.util.Utils.sum(a);

      d = new Dropout(units, 1.0);
      Arrays.fill(a, 1f);
      d.randomlySparsifyActivation(a, seed + 2);
      sum3 += water.util.Utils.sum(a);

      d = new Dropout(units, 0.314);
      d.fillBytes(seed+3);
//      Log.info("loop: " + l + " sum4: " + sum4);
      for (int i=0; i<units; ++i) {
        if (d.unit_active(i)) {
          sum4++;
          assert(d.unit_active(i));
        }
        else assert(!d.unit_active(i));
      }
//      Log.info(d.toString());
    }
    sum1 /= loops;
    sum2 /= loops;
    sum3 /= loops;
    sum4 /= loops;
    Assert.assertTrue(Math.abs(sum1-700)<1);
    Assert.assertTrue(sum2 == units);
    Assert.assertTrue(sum3 == 0);
    Assert.assertTrue(Math.abs(sum4-686)<1);
  }
}
