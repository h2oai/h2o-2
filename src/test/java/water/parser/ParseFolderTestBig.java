package water.parser;

import static org.junit.Assert.assertTrue;

import org.junit.Ignore;
import org.junit.Test;

import water.*;

public class ParseFolderTestBig extends TestUtil {

  @Test @Ignore("dataset directory is not usually available") public void  testCovtype(){
    Key k1 = null,k2 = null;
    try {
      k2 = loadAndParseFolder("multipart.hex","datasets/parse_folder_test" );
      k1 = loadAndParseFile("full.hex","datasets/UCI/UCI-large/covtype/covtype.data");
      Value v1 = DKV.get(k1);
      Value v2 = DKV.get(k2);
      assertTrue(v1.isBitIdentical(v2));
    } finally {
      if(k1 != null)UKV.remove(k1);
      if(k2 != null)UKV.remove(k2);
    }
  }
}
