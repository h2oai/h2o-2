package water.parser;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.ArrayList;
import org.junit.BeforeClass;
import org.junit.Test;
import water.*;
import water.util.FileIntegrityChecker;

public class ParseProgressTest extends TestUtil {

  @BeforeClass public static void stall() { stall_till_cloudsize(1); }

  // Attempt a multi-jvm parse of covtype.
  // Silently exits if it cannot find covtype.
  @Test public void testCovtype() {
    String[] covtype_locations = new String[]{"../datasets/UCI/UCI-large/covtype/covtype.data", "../datasets/UCI/UCI-large/covtype/covtype.data.gz",  "../demo/UCI-large/covtype/covtype.data", };
    File f = null;
    for (String covtype_location : covtype_locations) {
      f = find_test_file( covtype_location );
      if (f != null && f.exists())
        break;
    }
    if (f == null || !f.exists()) {
      System.out.println("Could not find covtype.data, skipping ParseProgressTest.testCovtype()");
      return;
    }

    FileIntegrityChecker c = FileIntegrityChecker.check(f,false);
    assertEquals(1,c.size());   // Exactly 1 file
    Key k = c.syncDirectory(null,null,null,null);
    assertEquals(true,k!=null);

    for( int i=0; i<1; i++ ) {
      Key covkey = Key.make("c"+i+".hex");
      ParseDataset.parse(covkey,new Key[]{k});
      ValueArray ary = DKV.get(covkey).get();
      ary.delete();
    }
  }
}
