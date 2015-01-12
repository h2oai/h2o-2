package water.fvec;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import water.*;
import water.fvec.Vec;
import static water.fvec.Vec.makeConSeq;
import static water.fvec.Vec.makeSeq;

import java.util.ArrayList;
import java.util.Arrays;


/** This test tests stability of Vec API. */
public class VecTest extends TestUtil {
  final int CHUNK_SZ = 1 << H2O.LOG_CHK;

  /** Test toEnum call to return correct domain. */
  @Test public void testToEnum() {
    testToEnumDomainMatch(vec(0,1,0,1), ar("0", "1") );
    testToEnumDomainMatch(vec(1,2,3,4,5,6,7), ar("1", "2", "3", "4", "5", "6", "7") );
    testToEnumDomainMatch(vec(-1,0,1,2,3,4,5,6), ar("-1", "0", "1", "2", "3", "4", "5", "6") );
  }

  private void testToEnumDomainMatch(Vec f, String[] expectedDomain) {
    Vec ef = null;
    try {
      ef = f.toEnum();
      String[] actualDomain = ef.domain();
      assertArrayEquals("toEnum call returns wrong domain!", expectedDomain, actualDomain);
    } finally {
      if (f!=null) UKV.remove(f._key);
      if (ef!=null) UKV.remove(ef._key);
    }
  }

  // want this to be test but avoid serialization of outer class (due to use of anonymous mr2s)
  private static final void testChangeDomainImpl(){
    ArrayList<Key> madeKeys = new ArrayList<Key>();
    try {
      final String [] oldDomain = {"a", "b", "c"};
      Vec v = Vec.makeNewCons(1000, 1, 0, new String[][]{oldDomain})[0];
      madeKeys.add(v._key);
      Vec.Writer vw = v.open();
      for (long i = 0; i < v.length(); ++i)
        vw.set(i, i % 3);
      vw.close();
      // now rebalance to ensure multiple chunks (and distribution to multiple nodes)
      Key reblancedKey = Key.make("reblanced");
      madeKeys.add(reblancedKey);
      RebalanceDataSet rbd = new RebalanceDataSet(new Frame(v), reblancedKey, 100);
      H2O.submitTask(rbd);
      rbd.join();
      Frame rebalancedFrame = DKV.get(reblancedKey).get();
      new MRTask2() {
        @Override public void map(Chunk c){
          assertTrue(Arrays.deepEquals(c._vec.domain(), oldDomain));
        }
      }.doAll(rebalancedFrame);
      madeKeys.add(rebalancedFrame.lastVec()._key);
      final String [] newDomain = new String[]{"x", "y", "z"};
      rebalancedFrame.lastVec().changeDomain(newDomain);
      new MRTask2() {
        @Override public void map(Chunk c){
          assertTrue(Arrays.deepEquals(c._vec.domain(), newDomain));
        }
      }.doAll(rebalancedFrame);
    } finally { for(Key k:madeKeys) UKV.remove(k);}
  }
  @Test public void testChangeDomain(){testChangeDomainImpl();}

  // Test HEX-1819
  @Test public void testMakeConSeq() {
    Vec v;

    v = makeConSeq(0xCAFE,CHUNK_SZ);
    assertTrue(v.at(234) == 0xCAFE);
    assertTrue(v._espc.length == 2);
    assertTrue(
            v._espc[0] == 0              &&
            v._espc[1] == CHUNK_SZ
    );
    v.remove(new Futures()).blockForPending();

    v = makeConSeq(0xCAFE,2*CHUNK_SZ);
    assertTrue(v.at(234) == 0xCAFE);
    assertTrue(v.at(2*CHUNK_SZ-1) == 0xCAFE);
    assertTrue(v._espc.length == 3);
    assertTrue(
            v._espc[0] == 0              &&
            v._espc[1] == CHUNK_SZ   &&
            v._espc[2] == CHUNK_SZ*2
    );
    v.remove(new Futures()).blockForPending();

    v = makeConSeq(0xCAFE,2*CHUNK_SZ+1);
    assertTrue(v.at(234) == 0xCAFE);
    assertTrue(v.at(2*CHUNK_SZ) == 0xCAFE);
    assertTrue(v._espc.length == 4);
    assertTrue(
            v._espc[0] == 0              &&
            v._espc[1] == CHUNK_SZ   &&
            v._espc[2] == CHUNK_SZ*2 &&
            v._espc[3] == CHUNK_SZ*2+1
    );
    v.remove(new Futures()).blockForPending();

    v = makeConSeq(0xCAFE,3*CHUNK_SZ);
    assertTrue(v.at(234) == 0xCAFE);
    assertTrue(v.at(3*CHUNK_SZ-1) == 0xCAFE);
    assertTrue(v._espc.length == 4);
    assertTrue(
            v._espc[0] == 0              &&
            v._espc[1] == CHUNK_SZ   &&
            v._espc[2] == CHUNK_SZ*2 &&
            v._espc[3] == CHUNK_SZ*3
    );
    v.remove(new Futures()).blockForPending();
  }
  // Test HEX-1819
  @Test public void testMakeSeq() {
    Vec v = makeSeq(3*CHUNK_SZ);
    assertTrue(v.at(0) == 1);
    assertTrue(v.at(234) == 235);
    assertTrue(v.at(2*CHUNK_SZ) == 2*CHUNK_SZ+1);
    assertTrue(v._espc.length == 4);
    assertTrue(
            v._espc[0] == 0 &&
            v._espc[1] == CHUNK_SZ &&
            v._espc[2] == CHUNK_SZ * 2 &&
            v._espc[3] == CHUNK_SZ * 3
    );
    v.remove(new Futures()).blockForPending();
  }
}
