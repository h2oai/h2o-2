package hex.gbm;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.Arrays;

import org.junit.BeforeClass;
import org.junit.Test;

import water.*;
import water.fvec.*;

public class GBMTest extends TestUtil {

  @BeforeClass public static void stall() { stall_till_cloudsize(1); }

  // ==========================================================================
  /*@Test*/ public void testBasicGBM() {
    File file = TestUtil.find_test_file("./smalldata/logreg/prostate.csv");
    Key fkey = NFSFileVec.make(file);
    Frame fr = ParseDataset2.parse(Key.make("prostate.hex"),new Key[]{fkey});
    UKV.remove(fkey);
    try {
      assertEquals(380,fr._vecs[0].length());

      // Prostate: predict on CAPSULE which is in column #1; move it to last column
      int ncols = fr._names.length;
      UKV.remove(fr.remove("ID")._key);   // Remove patient ID vector
      Vec capsule = fr.remove("CAPSULE"); // Remove capsule
      fr.add("CAPSULE",capsule);          // Move it to the end

      GBM gbm = GBM.start(GBM.makeKey(),fr,7);
      gbm.get();                  // Block for result
      UKV.remove(gbm._dest);
    } finally {
      UKV.remove(fr._key);
    }
  }
}
