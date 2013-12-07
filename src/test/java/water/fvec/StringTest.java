package water.fvec;

import static org.junit.Assert.*;
import java.io.File;
import org.junit.BeforeClass;
import org.junit.Test;
import water.*;
import water.parser.ParseDataset;

public class StringTest extends TestUtil {
  @BeforeClass public static void stall() { stall_till_cloudsize(2); }

  // ==========================================================================
  @Test public void testBasicCRUD() {
    // Parse a file with many broken enum/string columns
    Key k = Key.make("zip.hex");
    try {
      Frame fr = TestUtil.parseFrame(k,"../demo/bestbuy_test_200k.csv");
      //Frame fr = TestUtil.parseFrame(k,"smalldata/zip_code/zip_code_database.csv.gz");
      //Frame fr = TestUtil.parseFrame(k,"smalldata/tnc3.csv");
      System.out.println(fr);

      for( int i=0; i<fr.numCols(); i++ )
        System.out.println(fr.vecs()[i]);

      StringBuilder sb = new StringBuilder();
      String[] fs = fr.toStringHdr(sb);
      int lim = Math.min(40,(int)fr.numRows());
      for( int i=0; i<lim; i++ )
        fr.toString(sb,fs,i);
      System.out.println(sb.toString());
    } finally {
      UKV.remove(k);
    }
  }
}
