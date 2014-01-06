package water.parser;

import static org.junit.Assert.assertTrue;
import java.io.*;
import org.junit.Test;
import water.*;
import water.fvec.*;

public class ParseFolderTest extends TestUtil {

  @Test public void  testProstate(){
    Key k1 = null,k2 = null;
    try {
      Frame fr1 = parseFolder(k1=Key.make("multipart.hex"),new File("smalldata/parse_folder_test") );
      Frame fr2 = parseFrame (k2=Key.make("full.hex"),new File("smalldata/glm_test/prostate_cat_replaced.csv"));
      assertTrue("parsed values do not match!",fr1.dataEquals(fr2));
    } finally {
      if(k1 != null)UKV.remove(k1);
      if(k2 != null)UKV.remove(k2);
    }
  }
}
