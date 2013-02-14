package water.score;

import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;
import org.junit.Assert;
import org.junit.Test;
import water.parser.PMMLParser;
import water.util.TestUtil;
import water.*;

public class ScoreTest extends TestUtil {
  static final HashMap<String, Comparable> ROW;
  static {
    ROW = new HashMap<String, Comparable>();
    ROW.put("id", "80457298");
    ROW.put("string", "AOS");
    ROW.put("int", "71");
    ROW.put("long", 399L);
  }

  @Test
  public void testScorecard() throws Exception {
    File file = find_test_file("smalldata/pmml/Loan_Scorecard.xml");
    ScorecardModel scm = (ScorecardModel)PMMLParser.load(new FileInputStream(file));
    Assert.assertEquals(0, scm.score(ROW), 1e-6);
  }

  // Load and score a simple PMML RF model against the iris dataset.
  // Not ready, so not turned on.
  public void testRandomForest1() throws Exception {
    File file = find_test_file("smalldata/pmml/iris_rf_1tree.pmml.xml");
    ScoreModel rfm = PMMLParser.load(new FileInputStream(file));
  
    Key irisk = loadAndParseKey("iris.hex","smalldata/iris/iris.csv");
    ValueArray ary = ValueArray.value(DKV.get(irisk));
    AutoBuffer bits = ary.getChunk(0);
    int rows = ary.rpc(0);
    for( int i=0; i<rows; i++ ) {
      StringBuilder sb = new StringBuilder();
      for( int j=0; j<ary._cols.length; j++ )
        sb.append(ary._cols[j]._name).append("=").append(ary.datad(bits,i,j));
      sb.append("\n");
      System.err.println(sb.toString());
    }
  
  }
}
