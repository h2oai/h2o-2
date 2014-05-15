package water.util;

import static water.util.Utils.*;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;
import static water.TestUtil.ari;
import static water.TestUtil.arf;
public class UtilsTest {

  @Test public void testApproxMathImpl() {
    final int loops = 1000000;
    final long seed = new Random().nextLong();
    final float eps = 1e-20f;
    Random rng = new Random(seed);
    Log.info("Seed: " + seed);
    for (float maxVal : new float[]{1, Float.MAX_VALUE}) {
      Log.info("Testing " + loops + " numbers in interval [0, " + maxVal + "].");
      // float square
      {
        float err = 0;
        for (int i=0;i<loops;++i) {
          final float x = eps + rng.nextFloat() * maxVal;
          err = Math.max(Math.abs(err), Math.abs((float)Math.sqrt(x)-approxSqrt(x))/(float)Math.sqrt(x));
        }
        Log.info("rel. error for approxSqrt(float): " + err);
        Assert.assertTrue("rel. error for approxSqrt(float): " + err, Math.abs(err) < 5e-2);
      }

      // double square
      {
        double err = 0;
        for (int i=0;i<loops;++i) {
          final double x = eps + rng.nextFloat() * maxVal;
          err = Math.max(Math.abs(err), Math.abs(Math.sqrt(x)-approxSqrt(x))/Math.sqrt(x));
        }
        Log.info("rel. error for approxSqrt(double): " + err);
        Assert.assertTrue("rel. error for approxSqrt(double): " + err, Math.abs(err) < 5e-2);
      }

      // float inv square
      {
        float err = 0;
        for (int i=0;i<loops;++i) {
          final float x = eps + rng.nextFloat() * maxVal;
          err = Math.max(Math.abs(err), Math.abs((float)(1./Math.sqrt(x))-approxInvSqrt(x))*(float)Math.sqrt(x));
        }
        Log.info("rel. error for approxInvSqrt(float): " + err);
        Assert.assertTrue("rel. error for approxInvSqrt(float): " + err, Math.abs(err) < 2e-2);
      }

      // double inv square
      {
        double err = 0;
        for (int i=0;i<loops;++i) {
          final double x = eps + rng.nextFloat() * maxVal;
          err = Math.max(Math.abs(err), Math.abs((1./Math.sqrt(x))-approxInvSqrt(x))*Math.sqrt(x));
        }
        Log.info("rel. error for approxInvSqrt(double): " + err);
        Assert.assertTrue("rel. error for approxInvSqrt(double): " + err, Math.abs(err) < 2e-2);
      }

      // double exp
      {
        double err = 0;
        for (int i=0;i<loops;++i) {
          final double x = 30 - rng.nextDouble() * 60;
          err = Math.max(Math.abs(err), Math.abs(Math.exp(x)-approxExp(x))/Math.exp(x));
        }
        Log.info("rel. error for approxExp(double): " + err);
        Assert.assertTrue("rel. error for approxExp(double): " + err, Math.abs(err) < 5e-2);
      }

      // double log
      {
        double err = 0;
        for (int i=0;i<loops;++i) {
          final double x = eps + rng.nextFloat() * maxVal;
          err = Math.abs(Math.log(x)-approxLog(x))/Math.abs(Math.log(x));
          if (!Double.isInfinite(err) && !Double.isNaN(err))
            err = Math.max(err, Math.abs(Math.log(x)-approxLog(x))/Math.abs(Math.log(x)));
        }
        Log.info("rel. error for approxLog(double): " + err);
        Assert.assertTrue("rel. error for approxLog(double): " + err, Math.abs(err) < 1e-3);
      }
    }

    int[] idx = Utils.seq(0, 13 + new Random().nextInt(10000));
    int[] shuffled_idx = new int[idx.length];
    Utils.shuffleArray(idx, idx.length, shuffled_idx, new Random().nextLong(), 0);
    Utils.shuffleArray(idx, idx.length-13, shuffled_idx, new Random().nextLong(), 13);
  }

  @Test
  public void sumSquareTest() {
    float[] a = new float[993];
    for (int i=0;i<a.length;++i) a[i] = new Random(0xDECAF).nextFloat();
    Assert.assertTrue(Math.abs(sumSquares(a) - sumSquares(a, 0,443) - sumSquares(a, 443,983) - sumSquares(a, 983,993)) < 1e-5);
  }

  @Test
  public void testPartitione() {
    Assert.assertArrayEquals( ari(5,5),   Utils.partitione(10, arf(0.5f)) );
    Assert.assertArrayEquals( ari(5,5,0), Utils.partitione(10, arf(0.5f, 0.5f)) );
    Assert.assertArrayEquals( ari(6,7),   Utils.partitione(13, arf(0.5f)) );
    Assert.assertArrayEquals( ari(6,7,0), Utils.partitione(13, arf(0.5f, 0.5f)) );
    // more splits
    Assert.assertArrayEquals( ari(3,3,7),   Utils.partitione(13, arf(0.25f, 0.25f)) );
    Assert.assertArrayEquals( ari(3,3,7,0), Utils.partitione(13, arf(0.25f, 0.25f, 0.5f)) );
  }
}
