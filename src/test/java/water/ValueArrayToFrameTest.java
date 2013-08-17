package water;

import java.io.File;

import org.junit.*;

import water.fvec.Frame;

public class ValueArrayToFrameTest extends TestUtil {
  @BeforeClass public static void stall() {
    stall_till_cloudsize(3);
  }

  //@Test
   public void test() {
    test("smalldata/iris/iris.csv");
  }

//ks.add(loadAndParseFile("k" + ks.size(), "smalldata/covtype/covtype.20k.data"));
//ks.add(loadAndParseFile("k" + ks.size(), "smalldata/categoricals/30k_categoricals.csv.gz"));
//ks.add(loadAndParseFile("k" + ks.size(), "smalldata/unbalanced/orange_small_train.data.zip"));

  void test(String path) {
    File file = new File(path);
    Key key = null;
    Frame frame = null;
    try {
      key = loadAndParseFile(file.getName(), file.getPath());
      ValueArray va = UKV.get(key);
      frame = va.asFrame();

      for( int row = 0; row < va.numRows(); row++ ) {
        for( int i = 0; i < va._cols.length; i++ ) {
          if( va.isNA(row, i) )
            Assert.assertEquals(true, frame._vecs[i].isNA(row));
          else if( va._cols[i]._domain != null )
            Assert.assertEquals(va.data(row, i), frame._vecs[i].at8(row));
          else if( va._cols[i].isFloat() )
            Assert.assertEquals(va.datad(row, i), frame._vecs[i].at(row), 1e-8);
          else
            throw new RuntimeException("todo!");
        }
      }
    } finally {
      if( key != null )
        UKV.remove(key);
      if( frame != null )
        frame.remove();
    }
  }
}
