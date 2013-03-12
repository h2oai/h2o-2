package water.parser;

import static org.junit.Assert.assertEquals;
import java.io.File;
import java.io.FileInputStream;
import org.junit.*;
import water.*;
import water.RReader.RModel;

public class RReaderTest extends TestUtil {
  @BeforeClass
  public static void stall() {
    stall_till_cloudsize(1);
  }

  @Test
  public void testIrisModel() throws Exception {
    Key irisk = TestUtil.loadAndParseKey("iris.hex","smalldata/iris/iris2.csv");
    ValueArray iris = ValueArray.value(DKV.get(irisk));

    File file = TestUtil.find_test_file("smalldata/test/rmodels/iris_x-iris-1-4_y-species_ntree-500.rdata");
    Key key = Key.make("irisModel");
    RReader.run(key, new FileInputStream(file));
    RModel model = UKV.get(key, new RModel());
    int[] map = model.columnMapping(iris.colNames());
    Assert.assertTrue(Model.isCompatible(map));

    // Can I score on the model now?
    double[] row = new double[map.length];
    for( int i=0; i<iris._numrows; i++ ) {
      for( int j=0; j<map.length-1; j++ )
        row[j] = iris.datad(i,j);
      assertEquals(iris.datad(i,map.length-1),model.score(row,map),0.0001);
    }

    model.deleteKeys();
    UKV.remove(key);
    UKV.remove(irisk);
  }

  @Test
  public void testProstateModel() throws Exception {
    Key prok = TestUtil.loadAndParseKey("prostate.hex","smalldata/logreg/prostate.csv");
    ValueArray pro = ValueArray.value(DKV.get(prok));

    File file = TestUtil.find_test_file("smalldata/test/rmodels/prostate-rf-10tree-asFactorCapsule.rdata");
    Key key = Key.make("prostateModel");
    RReader.run(key, new FileInputStream(file));
    RModel model = UKV.get(key, new RModel());
    int[] map = model.columnMapping(pro.colNames());
    final int classCol = 1;     // Response column is known to be column #1, CAPSULE
    map[map.length-1] = classCol;

    // Can I score on the model now?
    double[] row = new double[map.length];
    int errs = 0;
    for( int i=0; i<pro._numrows; i++ ) {
      for( int j=0; j<map.length; j++ )
        row[j] = pro.datad(i,j);
      double score = model.score(row,map);
      if( Math.abs(pro.datad(i,classCol) - score) > 0.0001 ) errs++;
    }
    assertEquals(100,errs);

    model.deleteKeys();
    UKV.remove(key);
    UKV.remove(prok);
  }

  /*@Test*/
  public void testCovtypeModel() throws Exception {
    Key prok = TestUtil.loadAndParseKey("covtype.hex","smalldata/covtype/covtype.20k.data");
    ValueArray pro = ValueArray.value(DKV.get(prok));

    File file = TestUtil.find_test_file("smalldata/test/rmodels/covtype-rf-50tree-as-factor-X5.rdata");
    Key key = Key.make("covtypeModel");
    RReader.run(key, new FileInputStream(file));
    RModel model = UKV.get(key, new RModel());
    int[] map = new int[pro._cols.length];
    for( int i=0; i<map.length; i++ ) map[i] = i;
    final int classCol = map.length-1;     // Response column is known to be last column
    System.out.println("min="+pro._cols[classCol]._min);

    // Can I score on the model now?
    long start = System.currentTimeMillis();
    double[] row = new double[map.length];
    int errs = 0;
    for( int i=0; i<pro._numrows; i++ ) {
      for( int j=0; j<map.length; j++ )
        row[j] = pro.datad(i,j);
      double score = model.score(row,map);
      System.out.println(" "+i+" "+score+" "+(pro.datad(i,classCol)));
      if( Math.abs(pro.datad(i,classCol) - score) > 0.0001 ) errs++;
    }
    System.err.println(" errs= "+errs+"/"+pro._numrows);
    long now = System.currentTimeMillis();
    long dt = now-start;
    System.err.println("Took "+dt+"msec/"+pro._numrows+" = "+1000*dt/pro._numrows+" usec/row");

    model.deleteKeys();
    UKV.remove(key);
    UKV.remove(prok);
  }

}
