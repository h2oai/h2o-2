package water.api;

import org.junit.Assert;
import org.junit.Test;
import water.Key;
import water.TestUtil;
import water.fvec.Frame;

import java.util.Arrays;

public class ConfusionMatrixTest extends TestUtil {
  final boolean debug = false;

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
        debug);

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
        debug);
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
        debug);
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
         debug);

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
         debug);

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
         debug);
  }

  @Test
  public void testSimpleNumericVectors() {
    simpleCMTest(
        "smalldata/test/cm/v1n.csv",
        "smalldata/test/cm/v1n.csv",
        ar("0", "1", "2"),
        ar("0", "1", "2"),
        ar("0", "1", "2"),
        ar( ar(2L, 0L, 0L, 0L),
            ar(0L, 2L, 0L, 0L),
            ar(0L, 0L, 1L, 0L),
            ar(0L, 0L, 0L, 0L)
            ),
        debug);

    simpleCMTest(
        "smalldata/test/cm/v1n.csv",
        "smalldata/test/cm/v2n.csv",
        ar("0", "1", "2"),
        ar("0", "1", "2"),
        ar("0", "1", "2"),
        ar( ar(1L, 1L, 0L, 0L),
            ar(0L, 1L, 1L, 0L),
            ar(0L, 0L, 1L, 0L),
            ar(0L, 0L, 0L, 0L)
            ),
        debug);
  }

  @Test
  public void testDifferentDomainsNumericVectors() {

    simpleCMTest(
        "smalldata/test/cm/v1n.csv",
        "smalldata/test/cm/v4n.csv",
        ar("0", "1", "2"),
        ar("1", "2"),
        ar("0", "1", "2"),
        ar( ar(0L, 2L, 0L, 0L),
            ar(0L, 0L, 2L, 0L),
            ar(0L, 0L, 1L, 0L),
            ar(0L, 0L, 0L, 0L)
            ),
         debug);

    simpleCMTest(
        "smalldata/test/cm/v4n.csv",
        "smalldata/test/cm/v1n.csv",
        ar("1", "2"),
        ar("0", "1", "2"),
        ar("0", "1", "2"),
        ar( ar(0L, 0L, 0L, 0L),
            ar(2L, 0L, 0L, 0L),
            ar(0L, 2L, 1L, 0L),
            ar(0L, 0L, 0L, 0L)
            ),
         debug);

    simpleCMTest(
        "smalldata/test/cm/v2n.csv",
        "smalldata/test/cm/v4n.csv",
        ar("0", "1", "2"),
        ar("1", "2"),
        ar("0", "1", "2"),
        ar( ar(0L, 1L, 0L, 0L),
            ar(0L, 1L, 1L, 0L),
            ar(0L, 0L, 2L, 0L),
            ar(0L, 0L, 0L, 0L)
            ),
         debug);
  }

  /** Test for PUB-216:
   * The case when vector domain is set to a value (0~A, 1~B, 2~C), but actual values stored in
   * vector references only a subset of domain (1~B, 2~C). The TransfVec was using minimum from
   * vector (i.e., value 1) to compute transformation but minimum was wrong since it should be 0. */
  @Test public void testBadModelPrect() {

      simpleCMTest(
          frame("v1", vec(ar("A","B","C"), ari(0,0,1,1,2) )),
          frame("v2", vec(ar("A","B","C"), ari(1,1,2,2,2) )),
          ar("A","B","C"),
          ar("A","B","C"),
          ar("A","B","C"),
          ar( ar(0L, 2L, 0L, 0L),
              ar(0L, 0L, 2L, 0L),
              ar(0L, 0L, 1L, 0L),
              ar(0L, 0L, 0L, 0L)
              ),
          debug);

      simpleCMTest(
          frame("v1", vec(ar("B","C"), ari(0,0,1,1) )),
          frame("v2", vec(ar("A","B"), ari(1,1,0,0) )),
          ar("B","C"),
          ar("A","B"),
          ar("A","B","C"),
          ar( ar(0L, 0L, 0L, 0L), // A
              ar(0L, 2L, 0L, 0L), // B
              ar(2L, 0L, 0L, 0L), // C
              ar(0L, 0L, 0L, 0L)  // NA
              ),
          debug);
  }

  @Test public void testBadModelPrect2() {
      simpleCMTest(
          frame("v1", vec(ari(-1,-1,0,0,1) )),
          frame("v2", vec(ari( 0, 0,1,1,1) )),
          ar("-1","0","1"),
          ar("0","1"),
          ar("-1","0","1"),
          ar( ar(0L, 2L, 0L, 0L),
              ar(0L, 0L, 2L, 0L),
              ar(0L, 0L, 1L, 0L),
              ar(0L, 0L, 0L, 0L)
              ),
          debug);

      simpleCMTest(
          frame("v1", vec(ari(-1,-1,0,0) )),
          frame("v2", vec(ari( 1, 1,0,0) )),
          ar("-1","0"),
          ar("0","1"),
          ar("-1","0","1"),
          ar( ar(0L, 0L, 2L, 0L),
              ar(0L, 2L, 0L, 0L),
              ar(0L, 0L, 0L, 0L),
              ar(0L, 0L, 0L, 0L)
              ),
          debug);

      // The case found by Nidhi on modified covtype dataset
      simpleCMTest(
          frame("v1", vec(ari( 1, 2, 3, 4, 5, 6,  7) )),
          frame("v2", vec(ari( 1, 2, 3, 4, 5, 6, -1) )),
          ar(      "1","2","3","4","5","6","7"),
          ar("-1", "1","2","3","4","5","6"),
          ar("-1", "1","2","3","4","5","6","7"),
          ar( ar( 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L), // "-1"
              ar( 0L, 1L, 0L, 0L, 0L, 0L, 0L, 0L, 0L), // "1"
              ar( 0L, 0L, 1L, 0L, 0L, 0L, 0L, 0L, 0L), // "2"
              ar( 0L, 0L, 0L, 1L, 0L, 0L, 0L, 0L, 0L), // "3"
              ar( 0L, 0L, 0L, 0L, 1L, 0L, 0L, 0L, 0L), // "4"
              ar( 0L, 0L, 0L, 0L, 0L, 1L, 0L, 0L, 0L), // "5"
              ar( 0L, 0L, 0L, 0L, 0L, 0L, 1L, 0L, 0L), // "6"
              ar( 1L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L), // "7"
              ar( 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L)  // "NAs"
              ),
          debug);

      // Another case
      simpleCMTest(
          frame("v1", vec(ari( 7, 8,  9, 10, 11) )),
          frame("v2", vec(ari( 7, 8, 13, 10, 11) )),
          ar("7","8", "9","10","11"),
          ar("7","8","10","11","13"),
          ar("7","8","9","10","11","13"),
          ar( ar( 1L, 0L, 0L, 0L, 0L, 0L, 0L), // "7"
              ar( 0L, 1L, 0L, 0L, 0L, 0L, 0L), // "8"
              ar( 0L, 0L, 0L, 0L, 0L, 1L, 0L), // "9"
              ar( 0L, 0L, 0L, 1L, 0L, 0L, 0L), // "10"
              ar( 0L, 0L, 0L, 0L, 1L, 0L, 0L), // "11"
              ar( 0L, 0L, 0L, 0L, 0L, 0L, 0L), // "13"
              ar( 0L, 0L, 0L, 0L, 0L, 0L, 0L)  // "NAs"
              ),
          debug);

      // Mixed case
      simpleCMTest(
          frame("v1", vec(ar("-1", "1", "A"), ari( 0, 1, 2) )),
          frame("v2", vec(ar( "0", "1", "B"), ari( 0, 1, 2) )),
          ar("-1", "1", "A"),
          ar( "0", "1", "B"),
          ar( "-1", "0", "1", "A", "B"),
          ar( ar( 0L, 1L, 0L, 0L, 0L, 0L), // "-1"
              ar( 0L, 0L, 0L, 0L, 0L, 0L), // "0"
              ar( 0L, 0L, 1L, 0L, 0L, 0L), // "1"
              ar( 0L, 0L, 0L, 0L, 1L, 0L), // "A"
              ar( 0L, 0L, 0L, 0L, 0L, 0L), // "B"
              ar( 0L, 0L, 0L, 0L, 0L, 0L)  // "NAs"
              ),
          false);

      // Mixed case with change of numeric ordering 1, 10, 9 -> 1,9,10
      simpleCMTest(
          frame("v1", vec(ar("-1", "1", "10", "9", "A"), ari( 0, 1, 2, 3, 4) )),
          frame("v2", vec(ar( "0", "2",  "8", "9", "B"), ari( 0, 1, 2, 3, 4) )),
          ar("-1", "1", "10", "9", "A"),
          ar( "0", "2",  "8", "9", "B"),
          ar( "-1", "0", "1", "2",  "8", "9", "10", "A", "B"),
          ar( ar( 0L, 1L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L), // "-1"
              ar( 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L), // "0"
              ar( 0L, 0L, 0L, 1L, 0L, 0L, 0L, 0L, 0L, 0L), // "1"
              ar( 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L), // "2"
              ar( 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L), // "8"
              ar( 0L, 0L, 0L, 0L, 0L, 1L, 0L, 0L, 0L, 0L), // "9"
              ar( 0L, 0L, 0L, 0L, 1L, 0L, 0L, 0L, 0L, 0L), // "10"
              ar( 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 1L, 0L), // "A"
              ar( 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L), // "B"
              ar( 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L)  // "NAs"
              ),
          debug);
  }


  private void simpleCMTest(String f1, String f2, String[] expectedActualDomain, String[] expectedPredictDomain, String[] expectedDomain, long[][] expectedCM, boolean debug) {
    simpleCMTest(parseFrame(Key.make("v1.hex"), find_test_file(f1)), parseFrame(Key.make("v2.hex"), find_test_file(f2)), expectedActualDomain, expectedPredictDomain, expectedDomain, expectedCM, debug);
  }

  /** Delete v1, v2 after processing. */
  private void simpleCMTest(Frame v1, Frame v2, String[] expectedActualDomain, String[] expectedPredictDomain, String[] expectedDomain, long[][] expectedCM, boolean debug) {
    try {
      ConfusionMatrix cm = computeCM(v1, v2);
      // -- DEBUG --
      if (debug) {
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
    cm.invoke();
    return cm;
  }

}
