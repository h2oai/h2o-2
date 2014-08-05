package water;

import java.util.Random;

import junit.framework.Assert;

import org.junit.BeforeClass;
import org.junit.Test;

import water.util.Utils;

public class TestKeySnapshotLong extends TestUtil {
  @BeforeClass public static void stall() { stall_till_cloudsize(3); }

  /** Test for PUB-895 simulating Exec2 scenario.
   * <p>The scenario runs on 3JVMs and generates in single thread
   * keys, and takes snapshots (since each iteration of Exec2 invoked
   * from R rebuilds environment).
   * </p>
   * <p>The goal of the test is to make snaphost
   * at the time when the underlying NBHM is expanding</p>
   */
  @Test public void testGlobalKeySetIterativelly(){
    int i = 0;
    Random rng = new Random(0xa7a34721109d3708L ^ System.currentTimeMillis());
    try {
      for (i = 0; i < 50000; ++i) {
        UKV.put(Key.make("key" + i), new Utils.IcedInt(i));
        if (i % (rng.nextInt(31)+1) == 0) {
          Key[] keys2 = H2O.KeySnapshot.globalSnapshot().keys();
          Assert.assertEquals(i+1, keys2.length);
        }
      }
    } finally {
      Futures fs = new Futures();
      for (int j = 0; j < i; ++j) {
        DKV.remove(Key.make("key" + j),fs);
      }
      fs.blockForPending();
    }
  }
}
