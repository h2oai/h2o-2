package water.score;

import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;

import org.junit.Assert;
import org.junit.Test;

import test.TestUtil;
import water.parser.PMMLParser;

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
  public void run() throws Exception {
    File file = find_test_file("smalldata/pmml/Loan_Scorecard.xml");
    ScorecardModel scm = PMMLParser.load(new FileInputStream(file));
    Assert.assertEquals(0, scm.score(ROW), 1e-6);
  }
}
