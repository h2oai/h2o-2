package water;

import junit.framework.Assert;

import org.junit.BeforeClass;
import org.junit.Test;

import water.util.Utils;

public class TestKeySnapshotLong extends TestUtil {
  @BeforeClass public static void stall() { stall_till_cloudsize(3); }

  @Test
  public void testGlobalKeySetIterativelly(){
    int i = 0;
    try {
      for (i = 0; i < 50000; ++i) {
        UKV.put(Key.make("key" + i), new Utils.IcedInt(i));
        if (i % 10 == 0) {
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
