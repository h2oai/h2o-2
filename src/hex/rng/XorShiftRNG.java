package hex.rng;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Simple XorShiftRNG.
 *
 * Note: According to RF benchmarks it does not provide so accurate results
 * as {@link java.util.Random}, however it can be used as an alternative.
 *
 */
public class XorShiftRNG extends Random {

  private AtomicLong _seed;

  public XorShiftRNG (long seed) {
    this._seed = new AtomicLong(seed);
  }

  @Override
  public long nextLong() {
    long oldseed, nextseed;
    AtomicLong seed = this._seed;
    do {
      oldseed = seed.get();
      nextseed = xorShift(oldseed);
    } while (!seed.compareAndSet(oldseed, nextseed));

    return nextseed;
  }

  @Override
  public int nextInt() {
    return nextInt(Integer.MAX_VALUE);
  }

  @Override
  public int nextInt(int n) {
    int r = (int) (nextLong() % n);
    return r > 0 ? r : -r;
  }

  @Override
  protected int next(int bits) {
    long nextseed = nextLong();
    return (int) (nextseed & ((1L << bits) - 1));
  }

  private long xorShift(long x) {
    x ^= (x << 21);
    x ^= (x >>> 35);
    x ^= (x << 4);
    return x;
  }
}