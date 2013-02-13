package hex.rng;

import java.util.Random;

public class H2ORandomRNG extends Random {

  public H2ORandomRNG(long seed) {
    super();
    if ((seed >>> 32) < 0x0000ffffL) {
      //System.err.println("[H2ORandomRNG] correcting high-bits of seed");
      seed |= 0x5b93000000000000L;
    }
    if (((seed << 32) >>> 32) < 0x0000ffffL) {
      //System.err.println("[H2ORandomRNG] correcting low-bits of seed");
      seed |= 0xdb910000L;
    }
    setSeed(seed);
  }

  public enum RNGKind {
    DETERMINISTIC("deter", "determ"),
    NON_DETERMINISTIC("nondeter", "non-deter", "nondeterm", "non-determ");

    String[] shorcuts;

    private RNGKind(String... shortcuts) {  this.shorcuts = shortcuts; }

    public static RNGKind value(String s) {
      RNGKind[] kinds = values();
      for( RNGKind kind : kinds )
        for( String ss : kind.shorcuts )
          if( ss.equals(s) ) return kind;
      return RNGKind.valueOf(s);
    }

  }

  public enum RNGType {
    JavaRNG(RNGKind.DETERMINISTIC), MersenneTwisterRNG(RNGKind.DETERMINISTIC), XorShiftRNG(
        RNGKind.DETERMINISTIC), SecureRNG(RNGKind.NON_DETERMINISTIC);

    RNGKind kind;
    private RNGType(RNGKind kind) {  this.kind = kind; }
    public RNGKind kind() { return this.kind; }
  }
}
