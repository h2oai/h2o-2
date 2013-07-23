package water.fvec;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Ignore;
import org.junit.Test;

import water.Futures;

/** Test for CBSChunk implementation.
 *
 * The objective of the test is to verify compression method, not the H2O environment.
 *
 * NOTE: The test is attempt to not require H2O infrastructure to run.
 * It tries to use Mockito (perhaps PowerMock in the future) to wrap
 * expected results. In this case expectation is little bit missused
 * since it is used to avoid DKV call.
 * */
public class CBSChunkTest {

  void testImpl(long[] ls, int[] xs, int expBpv, int expGap, int expClen, int expNA) {
    // The following code mock underlying vector since we are not
    // tested them (=we are not interested in them), but chunk compression.
    // Mock the appendable vector.
    AppendableVec av = mock(AppendableVec.class);
    // Create an expectation - I know what should I expect
    // after closing the appendable vector.
    Vec vv = mock(Vec.class);
    vv.setNAs(Double.NaN, Long.MIN_VALUE);
    when(av.close(any(Futures.class))).thenReturn(vv);

    // Create a new chunk
    NewChunk nc = new NewChunk(av,0);
    nc._ls = ls;
    nc._xs = xs;
    nc._len = ls.length;
    for (int i=0;i<ls.length; i++) nc._naCnt += nc.isNA(i) ? 1 : 0; // Compute number of NAs
    assertEquals(expNA, nc._naCnt);
    // Compress chunk
    Chunk cc = nc.compress();
    cc._vec = av.close(new Futures());

    assertTrue( "Found chunk class "+cc.getClass()+" but expected " + CBSChunk.class, CBSChunk.class.isInstance(cc) );
    assertEquals(nc._len, cc._len);
    assertEquals(expGap, ((CBSChunk)cc)._gap);
    assertEquals(expBpv, ((CBSChunk)cc)._bpv);
    assertEquals(expClen, cc._mem.length - CBSChunk.OFF);
    // Also, we can decompress correctly
    for( int i=0; i<ls.length; i++ )
      assertEquals(xs[i]==0 ? ls[i] : Long.MIN_VALUE, cc.at80(i));
  }

  // Test one bit per value compression which is used
  // for data without NAs
  @Test @Ignore public void test1BPV() {
    // Simple case only compressing into 4bits of one byte
    testImpl(new long[] {0,0,0,1},
             new int [] {0,0,0,0},
             1, 4, 1, 0);
    // Filling whole byte
    testImpl(new long[] {1,0,0,0,1,1,1,0},
             new int [] {0,0,0,0,0,0,0,0},
             1, 0, 1, 0);
    // Crossing the border of two bytes by 1bit
    testImpl(new long[] {1,0,0,0,1,1,1,0, 1},
             new int [] {0,0,0,0,0,0,0,0, 0},
             1, 7, 2, 0);
  }

  // Test two bits per value compression used for case with NAs
  // used for data containing NAs
  @Test public void test2BPV() {
   // Simple case only compressing 2*3bits into 1byte including 1 NA
   testImpl(new long[] {0,0,1},
            new int [] {0,1,0},
            2, 2, 1, 1);
   // Filling whole byte, one NA
   testImpl(new long[] {1,0,0,1},
            new int [] {0,1,0,0},
            2, 0, 1, 1);
   // crossing the border of two bytes by 4bits, one NA
   testImpl(new long[] {1,0,0,1, 0,0},
            new int [] {0,0,1,0, 0,0},
            2, 4, 2, 1);
   // Two full bytes, 5 NAs
   testImpl(new long[] {0,0,0,1, 0,0,1,0},
            new int [] {1,1,1,0, 0,1,0,1},
            2, 0, 2, 5);
  }
}
