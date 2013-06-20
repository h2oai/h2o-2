package water.fvec;

import java.io.File;
import java.util.Arrays;
import org.junit.BeforeClass;
import org.junit.Test;
import water.Key;
import water.TestUtil;
import water.UKV;

public class GBMTest extends TestUtil {

  @BeforeClass public static void stall() { stall_till_cloudsize(1); }

  // ==========================================================================
  @Test public void testBasicGBM() {
    File file = TestUtil.find_test_file("../smalldata/logreg/umass_chdage.csv");
    Key fkey = NFSFileVec.make(file);

    Key okey = Key.make("chdage.hex");
    Frame fr = ParseDataset2.parse(okey,new Key[]{fkey});
    UKV.remove(fkey);
    System.out.println("Frame="+fr);

    UKV.remove(okey);
  }
}
