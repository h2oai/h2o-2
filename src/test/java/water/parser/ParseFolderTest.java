package water.parser;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import water.*;

public class ParseFolderTest extends TestUtil {

  @Test public void  testProstate(){
    Key k1 = null,k2 = null;
    try {
      k2 = loadAndParseFolder("multipart.hex","smalldata/parse_folder_test" );
      k1 = loadAndParseKey("full.hex","smalldata/glm_test/prostate_cat_replaced.csv");
      Value v1 = DKV.get(k1);
      Value v2 = DKV.get(k2);
      assertTrue(v1.isBitIdentical(v2));
    } finally {
      if(k1 != null)UKV.remove(k1);
      if(k2 != null)UKV.remove(k2);
    }
  }
}
