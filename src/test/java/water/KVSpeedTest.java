package water;

import org.junit.*;

// Weeny speed-test harness.  Not intended for use with any real testing.
public class KVSpeedTest extends TestUtil {

  @BeforeClass public static void stall() { stall_till_cloudsize(3); }

  /*@Test*/ public void test100Keys() {
    final int ITERS = 100000;
    Key   keys[] = new Key  [ITERS];
    Value vals[] = new Value[keys.length];
    for( int i=0; i<keys.length; i++ ) {
      Key k = keys[i] = Key.make("key"+i);
      Value v1 = vals[i] = new Value(k,"test2 bits for Value"+i);
      DKV.put(k,v1);
    }

    for( int i=0; i<10; i++ ) {
      long start = System.currentTimeMillis();
      impl_testKeys(ITERS,keys,vals);
      long now   = System.currentTimeMillis();
      System.out.println("(put+get+remove)/sec="+(now-start)+"ms / "+ITERS+
                         " = "+((double)(now-start)/ITERS));
    }
  }


  // ---
  // Make 100K keys, verify them all, delete them all.
  public void impl_testKeys(int iter,Key[]keys,Value[]vals) {
    for( int i=0; i<keys.length; i++ ) {
      Value v = DKV.get(keys[i]);
      assert v == vals[i];
    }
    //for( int i=0; i<keys.length; i++ ) {
    //  DKV.remove(keys[i]);
    //}
    //for( int i=0; i<keys.length; i++ ) {
    //  Value v3 = DKV.get(keys[i]);
    //  assertNull(v3);
    //}
  }
}
