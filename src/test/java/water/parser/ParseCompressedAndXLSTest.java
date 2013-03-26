package water.parser;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import water.*;

public class ParseCompressedAndXLSTest extends TestUtil {

  @Test public void  testIris(){
    Key k1 = null,k2 = null,k3 = null, k4 = null;
    try {
      k1 = loadAndParseKey("csv.hex","smalldata/iris/iris_wheader.csv");
      k2 = loadAndParseKey("xls.hex","smalldata/iris/iris.xls");
      k3 = loadAndParseKey("gzip.hex","smalldata/iris/iris_wheader.csv.gz");
      k4 = loadAndParseKey("zip.hex","smalldata/iris/iris_wheader.csv.zip");
      Value v1 = DKV.get(k1);
      Value v2 = DKV.get(k2);
      Value v3 = DKV.get(k3);
      Value v4 = DKV.get(k4);
      assertTrue(v1.isBitIdentical(v2));
      assertTrue(v2.isBitIdentical(v3));
      assertTrue(v3.isBitIdentical(v4));
    } finally {
      if(k1 != null)UKV.remove(k1);
      if(k2 != null)UKV.remove(k2);
      if(k3 != null)UKV.remove(k3);
      if(k4 != null)UKV.remove(k4);
    }
  }
}
