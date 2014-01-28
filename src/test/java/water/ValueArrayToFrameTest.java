package water;

import java.io.File;

import org.junit.*;

import water.ValueArray.CsvVAStream;
import water.fvec.Frame;
import water.util.Utils;

public class ValueArrayToFrameTest extends TestUtil {
  @BeforeClass public static void stall() {
    stall_till_cloudsize(3);
  }

  @Test public void iris() {
    test("smalldata/iris/iris.csv");
  }

  @Test public void covtype() {
    test("smalldata/covtype/covtype.20k.data");
  }

  @Test public void categoricals() {
    test("smalldata/categoricals/30k_categoricals.csv.gz");
  }

  @Test public void constantColumn() {
    test("smalldata/constantColumn.csv");
  }

  static void test(String path) {
    File file = new File(path);
    Key key = null, key1 = null, key2 = null;
    Frame frame = null;
    try {
      key = loadAndParseFile(file.getName(), file.getPath());
      ValueArray va = UKV.get(key);
      File csv1 = File.createTempFile("h2o", null);
      Utils.writeFileAndClose(csv1, new CsvVAStream(va, null));

      frame = va.asFrame();
      File csv2 = File.createTempFile("h2o", null);
      Utils.writeFileAndClose(csv2, frame.toCSV(true));

      key1 = loadAndParseFile(csv1.getName(), csv1.getPath());
      key2 = loadAndParseFile(csv2.getName(), csv2.getPath());
      ValueArray va1 = UKV.get(key);
      ValueArray va2 = UKV.get(key);
      Assert.assertEquals(va._numrows, va1._numrows);
      Assert.assertEquals(va._cols.length, va1._cols.length);
      Assert.assertEquals(va._numrows, va2._numrows);
      Assert.assertEquals(va._cols.length, va2._cols.length);
      Value v1 = DKV.get(key1);
      Value v2 = DKV.get(key2);
      Assert.assertTrue(v1.isBitIdentical(v2));
    } catch( Exception e ) {
      throw new RuntimeException(e);
    } finally {
      Lockable.delete(key);
      Lockable.delete(key1);
      Lockable.delete(key2);
      if( frame != null ) frame.delete();
    }
  }
}
