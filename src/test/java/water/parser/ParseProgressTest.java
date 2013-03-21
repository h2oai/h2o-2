package water.parser;

import static org.junit.Assert.assertEquals;
import java.io.IOException;
import java.io.File;
import java.util.Arrays;
import org.junit.*;
import water.*;
import water.parser.ParseDataset;
import water.util.FileIntegrityChecker;

public class ParseProgressTest extends TestUtil {

  @BeforeClass public static void stall() { stall_till_cloudsize(1); }

  // Attempt a multi-jvm parse of covtype.
  // Silently exits if it cannot find covtype.
  @Test public void testCovtype() {
    File f = find_test_file("../datasets/UCI/UCI-large/covtype/covtype.data");
    if( !f.exists() )
      f = find_test_file("../demo/UCI-large/covtype/covtype.data");
    if( !f.exists() )
      return;
    FileIntegrityChecker c = FileIntegrityChecker.check(f);
    assertEquals(1,c.size());   // Exactly 1 file
    Key k = c.importFile(0, null);
    assertEquals(true,k!=null);

    for( int i=0; i<1; i++ ) {
      Key covkey = Key.make("c"+i+".hex");
      ParseDataset.parse(covkey,DKV.get(k));
      ValueArray ary = DKV.get(covkey).get();
      UKV.remove(covkey);
    }

    UKV.remove(k);
  }
}
