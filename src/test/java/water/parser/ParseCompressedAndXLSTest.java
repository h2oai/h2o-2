package water.parser;

import static org.junit.Assert.assertTrue;
import java.util.Arrays;
import org.junit.Test;
import water.*;
import water.fvec.Frame;

public class ParseCompressedAndXLSTest extends TestUtil {

  @Test public void  testIris(){
    Key k1 = null,k2 = null,k3 = null, k4 = null;
    try {
      Frame fr1 = parseFrame(k1 = Key.make( "csv.hex"),"smalldata/iris/iris_wheader.csv");
      Frame fr2 = parseFrame(k2 = Key.make( "xls.hex"),"smalldata/iris/iris.xls");
      Frame fr3 = parseFrame(k3 = Key.make("gzip.hex"),"smalldata/iris/iris_wheader.csv.gz");
      Frame fr4 = parseFrame(k4 = Key.make( "zip.hex"),"smalldata/iris/iris_wheader.csv.zip");
      assertTrue(fr1.dataEquals(fr2));
      assertTrue(fr2.dataEquals(fr3));
      assertTrue(fr3.dataEquals(fr4));
    } finally {
      if(k1 != null)UKV.remove(k1);
      if(k2 != null)UKV.remove(k2);
      if(k3 != null)UKV.remove(k3);
      if(k4 != null)UKV.remove(k4);
    }
  }
}
