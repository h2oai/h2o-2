package water.score;

import java.io.*;
import java.util.HashMap;
import org.junit.Assert;
import org.junit.Test;
import water.parser.PMMLParser;
import water.*;

public class ScoreTest extends TestUtil {
  static final HashMap<String, Comparable> ROW;
  static {
    ROW = new HashMap<String, Comparable>();
    ROW.put("id", "80457298");
    ROW.put("name", "AOS");
    ROW.put("age", "71");
    ROW.put("income", 399L);
  }

  @Test
  public void testScorecard() throws Exception {
    File file = find_test_file("smalldata/pmml/SampleScorecard.pmml");
    ScorecardModel scm = (ScorecardModel)PMMLParser.parse(new FileInputStream(file));
    Assert.assertEquals(5.505753, scm.score_interpreter(ROW), 1e-6);
    Assert.assertEquals(5.505753, scm.score(ROW), 1e-6);
  }

  // Load and score a simple PMML RF model against the iris dataset.
  @Test
  public void testRandomForest1() throws Exception {
    // The 1-tree set scores with 2 errors.
    // The 500-tree set scores perfectly, takes slightly long to load.
    //File file = find_test_file("smalldata/pmml/iris_rf_500trees.pmml");
    File file = find_test_file("smalldata/pmml/iris_rf_1tree.pmml");
    //File file = find_test_file("smalldata/pmml/cars-cater-rf-1tree.pmml");
    RFScoreModel rfm = (RFScoreModel)PMMLParser.parse(new BufferedInputStream(new FileInputStream(file)));
  
    Key irisk = loadAndParseKey("iris.hex","smalldata/iris/iris2.csv");
    ValueArray ary = ValueArray.value(DKV.get(irisk));
    AutoBuffer bits = ary.getChunk(0);
    int rows = ary.rpc(0);
    int errs = 0;
    for( int i=0; i<rows; i++ ) {
      HashMap<String,Comparable> row = new HashMap();
      for( int j=0; j<ary._cols.length; j++ )
        row.put(ary._cols[j]._name,ary.datad(bits,i,j));
      int pr = (int)rfm.score(row);
      int ac = (int)ary.data(bits,i,4);
      if( pr != ac )
        errs++;
    }
    Assert.assertEquals(2,errs);
    UKV.remove(irisk);
  }
}
