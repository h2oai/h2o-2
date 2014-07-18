package water.parser;

import static org.junit.Assert.assertTrue;
import org.junit.Test;
import water.*;

public class ParseFolderTest extends TestUtil {

  @Test public void testProstate() {
    Key k1 = null,k2 = null;
    try {
      k2 = loadAndParseFolder("multipart.hex","smalldata/parse_folder_test" );
      k1 = loadAndParseFile("full.hex","smalldata/glm_test/prostate_cat_replaced.csv");
      Value v1 = DKV.get(k1);
      Value v2 = DKV.get(k2);
      assertTrue("parsed values do not match!",v1.isBitIdentical(v2));
    } finally {
      Lockable.delete(k1);
      Lockable.delete(k2);
    }
  }
}
