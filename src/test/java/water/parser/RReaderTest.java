package water.parser;

import static org.junit.Assert.assertEquals;
import hex.rf.RFModel;
import java.io.File;
import java.io.FileInputStream;
import org.junit.*;
import water.*;

public class RReaderTest extends TestUtil {
  @BeforeClass
  public static void stall() {
    stall_till_cloudsize(1);
  }

  @Test
  public void testIrisModel() throws Exception {
    Key irisk = TestUtil.loadAndParseFile("iris.hex","smalldata/iris/iris2.csv");
    ValueArray iris = DKV.get(irisk).get();

    File file = TestUtil.find_test_file("smalldata/test/rmodels/iris_x-iris-1-4_y-species_ntree-500.rdata");
    Key key = Key.make("irisModel");
    RReader.run(key, new FileInputStream(file));
    RFModel model = UKV.get(key);
    OldModel m = model.adapt(iris.colNames());

    // Can I score on the model now?
    double[] row = new double[iris._cols.length];
    for( int i=0; i<iris._numrows; i++ ) {
      for( int j=0; j<iris._cols.length-1; j++ )
        row[j] = iris.datad(i,j);
      assertEquals(iris.datad(i,iris._cols.length-1),m.score(row),0.0001);
    }
    m.delete();
    if( m != model ) model.delete();
    iris.delete();
  }

  @Test
  public void testProstateModel() throws Exception {
    Key prok = TestUtil.loadAndParseFile("prostate.hex","smalldata/logreg/prostate.csv");
    ValueArray pro = DKV.get(prok).get();

    File file = TestUtil.find_test_file("smalldata/test/rmodels/prostate-rf-10tree-asFactorCapsule.rdata");
    Key key = Key.make("prostateModel");
    RReader.run(key, new FileInputStream(file));
    RFModel model = UKV.get(key);
    int[] map = model.columnMapping(pro.colNames());
    final int classCol = 1;     // Response column is known to be column #1, CAPSULE
    map[map.length-1] = classCol;

    // Can I score on the model now?
    double[] row = new double[map.length];
    int errs = 0;
    OldModel M = model.adapt(pro);
    for( int i=0; i<pro._numrows; i++ ) {
      for( int j=0; j<map.length; j++ )
        row[j] = pro.datad(i,j);
      double score = M.score(row);
      if( Math.abs(pro.datad(i,classCol) - score) > 0.0001 ) errs++;
    }
    assertEquals(100,errs);

    M.delete();
    model.delete();
    pro.delete();
  }

  /*@Test*/
  public void testCovtypeModel() throws Exception {
    Key prok = TestUtil.loadAndParseFile("covtype.hex","smalldata/covtype/covtype.20k.data");
    ValueArray pro = DKV.get(prok).get();

    File file = TestUtil.find_test_file("smalldata/test/rmodels/covtype-rf-50tree-as-factor-X5.rdata");
    Key key = Key.make("covtypeModel");
    RReader.run(key, new FileInputStream(file));
    RFModel model = UKV.get(key);
    int[] map = new int[pro._cols.length];
    for( int i=0; i<map.length; i++ ) map[i] = i;
    final int classCol = map.length-1;     // Response column is known to be last column
    System.out.println("min="+pro._cols[classCol]._min);

    // Can I score on the model now?
    long start = System.currentTimeMillis();
    double[] row = new double[map.length];
    OldModel M = model.adapt(pro);
    int errs = 0;
    for( int i=0; i<pro._numrows; i++ ) {
      for( int j=0; j<map.length; j++ )
        row[j] = pro.datad(i,j);
      double score = M.score(row);
      System.out.println(" "+i+" "+score+" "+(pro.datad(i,classCol)));
      if( Math.abs(pro.datad(i,classCol) - score) > 0.0001 ) errs++;
    }
    System.err.println(" errs= "+errs+"/"+pro._numrows);
    long now = System.currentTimeMillis();
    long dt = now-start;
    System.err.println("Took "+dt+"msec/"+pro._numrows+" = "+1000*dt/pro._numrows+" usec/row");

    M.delete();
    model.delete();
    pro.delete();
  }

}
