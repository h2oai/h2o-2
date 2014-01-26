package water.parser;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import water.*;

public class ParseCompressedAndXLSTest extends TestUtil {

  @Test public void  testIris(){
    Key k1 = null,k2 = null,k3 = null, k4 = null;
    try {
      k1 = loadAndParseFile("csv.hex","smalldata/iris/iris_wheader.csv");
      k2 = loadAndParseFile("xls.hex","smalldata/iris/iris.xls");
      k3 = loadAndParseFile("gzip.hex","smalldata/iris/iris_wheader.csv.gz");
      k4 = loadAndParseFile("zip.hex","smalldata/iris/iris_wheader.csv.zip");
      Value v1 = DKV.get(k1);
      Value v2 = DKV.get(k2);
      Value v3 = DKV.get(k3);
      Value v4 = DKV.get(k4);
      assertTrue(v1.isBitIdentical(v2));
      assertTrue(v2.isBitIdentical(v3));
      assertTrue(v3.isBitIdentical(v4));
    } finally {
      Lockable.delete(k1);
      Lockable.delete(k2);
      Lockable.delete(k3);
      Lockable.delete(k4);
    }
  }
}
