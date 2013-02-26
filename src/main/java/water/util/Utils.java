package water.util;

import hex.rng.*;
import hex.rng.H2ORandomRNG.RNGKind;
import hex.rng.H2ORandomRNG.RNGType;

import java.io.Closeable;
import java.io.IOException;
import java.net.Socket;
import java.security.SecureRandom;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.Random;

public class Utils {

  /** Returns the index of the largest value in the array.
   * In case of a tie, an the index is selected randomly.
   */
  public static int maxIndex(int[] from, Random rand) {
    assert rand != null;
    int result = 0;
    int maxCount = 0; // count of maximal element for a 1 item reservoir sample
    for( int i = 1; i < from.length; ++i ) {
      if( from[i] > from[result] ) {
        result = i;
        maxCount = 1;
      } else if( from[i] == from[result] ) {
        if( rand.nextInt(++maxCount) == 0 ) result = i;
      }
    }
    return result;
  }

  public static int maxIndex(int[] from) {
    int result = 0;
    for (int i = 1; i<from.length; ++i)
      if (from[i]>from[result]) result = i;
    return result;
  }

  public static double lnF(double what) {
    return (what < 1e-06) ? 0 : what * Math.log(what);
  }

  public static String p2d(double d) { return new DecimalFormat ("0.##"   ).format(d); }
  public static String p5d(double d) { return new DecimalFormat ("0.#####").format(d); }

  public static int set4( byte[] buf, int off, int x ) {
    for( int i=0; i<4; i++ ) buf[i+off] = (byte)(x>>(i<<3));
    return 4;
  }
  public static int get4( byte[] buf, int off ) {
    int sum=0;
    for( int i=0; i<4; i++ ) sum |= (0xff&buf[off+i])<<(i<<3);
    return sum;
  }

  public static int set8d( byte[] buf, int off, double d ) {
    long x = Double.doubleToLongBits(d);
    for( int i=0; i<8; i++ ) buf[i+off] = (byte)(x>>(i<<3));
    return 8;
  }
  public static double get8d( byte[] buf, int off ) {
    long sum=0;
    for( int i=0; i<8; i++ ) sum |= ((long)(0xff&buf[off+i]))<<(i<<3);
    return Double.longBitsToDouble(sum);
  }

  public static int sum(int[] from) {
    int result = 0;
    for (int d: from) result += d;
    return result;
  }

  public static void pln(String s) { System.out.println(s); }

  public static String sampleToString(int[] val, int max) {
    if (val == null || val.length < max) return Arrays.toString(val);

    StringBuilder b = new StringBuilder();
    b.append('[');
    max -= 10;
    int valMax = val.length -1;
    for (int i = 0; ; i++) {
        b.append(val[i]);
        if (i == max) {
          b.append(", ...");
          i = val.length - 10;
        }
        if ( i == valMax) {
          return b.append(']').toString();
        }
        b.append(", ");
    }
  }

  /* Always returns a deterministic java.util.Random RNG.
   *
   * The determinism is important for re-playing sampling.
   */
  public static Random getDeterRNG(long seed) { return new H2ORandomRNG(seed); }

  public static void setUsedRNGKind(final RNGKind kind) {
    switch (kind) {
    case DETERMINISTIC:
      setUsedRNGType(RNGType.MersenneTwisterRNG);
      break;
    case NON_DETERMINISTIC:
      setUsedRNGType(RNGType.SecureRNG);
      break;
    }
  }

  /* Returns the configured random generator */
  public synchronized static Random getRNG(long... seed) {
    assert _rngType != null : "Random generator type has to be configured";
    switch (_rngType) {
    case JavaRNG:
      assert seed.length >= 1;
      return new H2ORandomRNG(seed[0]);
    case MersenneTwisterRNG:
      // do not copy the seeds - use them, and initialize the first two ints by seeds based given argument
      // the call is locked, and also MersenneTwisterRNG will just copy the seeds into its datastructures
      assert seed.length == 1;
      int[] seeds    = MersenneTwisterRNG.SEEDS;
      int[] inSeeds = unpackInts(seed);
      seeds[0] = inSeeds[0];
      seeds[1] = inSeeds[1];
      return new MersenneTwisterRNG(seeds);
    case XorShiftRNG:
      assert seed.length >= 1;
      return new XorShiftRNG(seed[0]);
    case SecureRNG:
      return new SecureRandom();
    }

    throw new IllegalArgumentException("Unknown random generator type: " + _rngType);
  }

  private static RNGType _rngType = RNGType.MersenneTwisterRNG;

  public static void setUsedRNGType(RNGType rngType) {
    Utils._rngType = rngType;
  }

  public static RNGType getUsedRNGType() {
    return Utils._rngType;
  }

  public static RNGKind getUsedRNGKind() {
    return Utils._rngType.kind();
  }

  /*
   * Compute entropy value for an array of bytes.
   *
   * The returned number represents entropy per bit!
   * For good long number seed (8bytes seed) it should be in range <2.75,3> (higher is better)
   *
   * For large set of bytes (>100) it should be almost 8 (means almost 8 random bits per byte).
   */
  public static float entropy(byte[] f) {
    int counts[] = new int[256];
    float entropy = 0;
    float total = f.length;

    for (byte b : f) counts[b+128]++;
    for (int c : counts) {
      if (c == 0) continue;
      float p = c / total;

      /* Compute entropy per bit in byte.
       *
       * To compute entropy per byte compute log with base 256 = log(p)/log(256).
       */
      entropy -= p * Math.log(p)/Math.log(2);
    }

    return entropy;
  }

  public static int[] unpackInts(long... longs) {
    int len      = 2*longs.length;
    int result[] = new int[len];
    int i = 0;
    for (long l : longs) {
      result[i++] = (int) (l & 0xffffffffL);
      result[i++] = (int) (l>>32);
    }
    return result;
  }

  public static void shuffleArray(long[] a) {
    int n = a.length;
    Random random = new Random();
    random.nextInt();
    for (int i = 0; i < n; i++) {
      int change = i + random.nextInt(n - i);
      swap(a, i, change);
    }
  }

  private static void swap(long[] a, int i, int change) {
    long helper = a[i];
    a[i] = a[change];
    a[change] = helper;
  }

  public static void close(Closeable closeable) {
    try { if( closeable != null ) closeable.close(); } catch( IOException _ ) { }
  }

  public static void close(Socket s) {
    try { if( s != null ) s.close(); } catch( IOException _ ) { }
  }

  public static int nextPowerOf2(int value) {
    value--;
    value |= value >> 1;
    value |= value >> 2;
    value |= value >> 4;
    value |= value >> 8;
    value |= value >> 16;
    value++;
    return value;
  }
}
