package water.api;

import org.junit.Assert;
import org.junit.Test;
import water.Key;
import water.TestUtil;
import water.fvec.Frame;

import java.util.Arrays;

public class ConfusionMatrixTest extends TestUtil {

  @Test
  public void testIdenticalVectors() {

    simpleCMTest(
        "smalldata/test/cm/v1.csv",
        "smalldata/test/cm/v1.csv",
        ar("A", "B", "C"),
        ar("A", "B", "C"),
        ar("A", "B", "C"),
        ar( ar(2L, 0L, 0L, 0L),
            ar(0L, 2L, 0L, 0L),
            ar(0L, 0L, 1L, 0L),
            ar(0L, 0L, 0L, 0L)
            ),
        false);

  }


  @Test
  public void testVectorAlignment() {

    simpleCMTest(
        "smalldata/test/cm/v1.csv",
        "smalldata/test/cm/v2.csv",
        ar("A", "B", "C"),
        ar("A", "B", "C"),
        ar("A", "B", "C"),
        ar( ar(1L, 1L, 0L, 0L),
            ar(0L, 1L, 1L, 0L),
            ar(0L, 0L, 1L, 0L),
            ar(0L, 0L, 0L, 0L)
            ),
        false);
  }

  /** Negative test testing expected exception if two vectors
   * of different lengths are provided.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testDifferentLenghtVectors() {

    simpleCMTest(
        "smalldata/test/cm/v1.csv",
        "smalldata/test/cm/v3.csv",
        ar("A", "B", "C"),
        ar("A", "B", "C"),
        ar("A", "B", "C"),
        ar( ar(1L, 1L, 0L, 0L),
            ar(0L, 1L, 1L, 0L),
            ar(0L, 0L, 1L, 0L),
            ar(0L, 0L, 0L, 0L)
            ),
        false);
  }

  @Test
  public void testDifferentDomains() {

    simpleCMTest(
        "smalldata/test/cm/v1.csv",
        "smalldata/test/cm/v4.csv",
        ar("A", "B", "C"),
        ar("B", "C"),
        ar("A", "B", "C"),
        ar( ar(0L, 2L, 0L, 0L),
            ar(0L, 0L, 2L, 0L),
            ar(0L, 0L, 1L, 0L),
            ar(0L, 0L, 0L, 0L)
            ),
         false);

    simpleCMTest(
        "smalldata/test/cm/v4.csv",
        "smalldata/test/cm/v1.csv",
        ar("B", "C"),
        ar("A", "B", "C"),
        ar("A", "B", "C"),
        ar( ar(0L, 0L, 0L, 0L),
            ar(2L, 0L, 0L, 0L),
            ar(0L, 2L, 1L, 0L),
            ar(0L, 0L, 0L, 0L)
            ),
         false);

    simpleCMTest(
        "smalldata/test/cm/v2.csv",
        "smalldata/test/cm/v4.csv",
        ar("A", "B", "C"),
        ar("B", "C"),
        ar("A", "B", "C"),
        ar( ar(0L, 1L, 0L, 0L),
            ar(0L, 1L, 1L, 0L),
            ar(0L, 0L, 2L, 0L),
            ar(0L, 0L, 0L, 0L)
            ),
         false);
  }


  private void simpleCMTest(String f1, String f2, String[] expectedActualDomain, String[] expectedPredictDomain, String[] expectedDomain, long[][] expectedCM, boolean debug) {
    Frame v1 = null, v2 = null;
    try {
      v1 = parseFrame(Key.make("v1.hex"), find_test_file(f1));
      v2 = parseFrame(Key.make("v2.hex"), find_test_file(f2));
      ConfusionMatrix cm = computeCM(v1, v2);
      // -- DEBUG --
      if (true) {
        System.err.println(Arrays.toString(cm.actual_domain));
        System.err.println(Arrays.toString(cm.predicted_domain));
        for (int i=0; i<cm.cm.length; i++)
          System.err.println(Arrays.toString(cm.cm[i]));
        StringBuilder sb = new StringBuilder();
        cm.toASCII(sb);
        System.err.println(sb.toString());
      }
      // -- -- --
      assertCMEqual(expectedActualDomain,
                    expectedPredictDomain,
                    expectedDomain,
                    expectedCM,
                    cm);
    } finally {
      if (v1 != null) v1.delete();
      if (v2 != null) v2.delete();
    }
  }

  private void assertCMEqual(String[] expectedActualDomain, String[] expectedPredictDomain, String[] expectedDomain, long[][] expectedCM, ConfusionMatrix actualCM) {
    Assert.assertArrayEquals("Actual CM domain differs",    expectedActualDomain,  actualCM.actual_domain);
    Assert.assertArrayEquals("Predicted CM domain differs", expectedPredictDomain, actualCM.predicted_domain);
    Assert.assertArrayEquals("Expected domain differs",     expectedDomain,        actualCM.domain);
    long[][] acm = actualCM.cm;
    Assert.assertEquals("CM dimension differs", expectedCM.length, acm.length);
    for (int i=0; i < acm.length; i++) Assert.assertArrayEquals("CM row " +i+" differs!", expectedCM[i], acm[i]);
  }

  private ConfusionMatrix computeCM(Frame v1, Frame v2) {
    assert v1.vecs().length == 1 && v2.vecs().length == 1 : "Test expect single vector frames!";
    ConfusionMatrix cm = new ConfusionMatrix();
    cm.actual   = v1;
    cm.vactual  = v1.vecs()[0];
    cm.predict  = v2;
    cm.vpredict = v2.vecs()[0];
    // Ohh nooo, this is block call :-)
    // Finally time for joke:
    //  """ Two men walk into a bar. The first one says "I'll have some H2O." The next man says "I'll have some H2O too" :-D """
    cm.serve();
    return cm;
  }

}
