package water;

import dontweave.gson.*;
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
      impl_testKeys(keys,vals);
      long now   = System.currentTimeMillis();
      System.out.println("(put+get+remove+get)/sec="+(now-start)+"ms / "+ITERS+
                         " = "+((double)(now-start)/ITERS));
    }
    for( Key key : keys ) DKV.remove(key);
  }


  // ---
  // Make 100K keys, verify them all, delete them all.
  public void impl_testKeys(Key[]keys,Value[]vals) {
    //for( int i=0; i<keys.length; i++ ) {
    //  Key k = keys[i];
    //  Value v1 = vals[i] = new Value(k,"test2 bits for Value"+i);
    //  DKV.put(k,v1);
    //}
    for( int i=0; i<keys.length; i++ ) {
      Value v = DKV.get(keys[i]);
      assert v == vals[i];
    }
    //for( int i=0; i<keys.length; i++ ) {
    //  DKV.remove(keys[i]);
    //}
    //for( int i=0; i<keys.length; i++ ) {
    //  Value v3 = DKV.get(keys[i]);
    //  Assert.assertNull(v3);
    //}
  }

  @Test @Ignore public void dummy_test() {
    /* this is just a dummy test to avoid JUnit complains about missing test */
  }


  // Inject a million system keys and a dozen user keys around the cluster.
  // Verify that StoreView and TypeAhead remain fast.
  @Test public void fastGlobalKeySearch() {
    final long t_start = System.currentTimeMillis();
    final int NUMKEYS=100;  // fast test for junits
    //final int NUMKEYS=1000000;  // a million keys
    new DoKeys(true ,NUMKEYS,15).invokeOnAllNodes();
    final long t_make = System.currentTimeMillis();

    // Skip 1st 10 keys of a StoreView.  Return the default of 20 more
    // user-mode keys.
    String json = new water.api.StoreView().setAndServe("10");
    //System.out.println(json);
    final long t_view = System.currentTimeMillis();

    new DoKeys(false,NUMKEYS,15).invokeOnAllNodes();
    final long t_remove = System.currentTimeMillis();
    //System.out.print("Make: "+((t_make  -t_start)*1.0/NUMKEYS)+"\n"+
    //                 "View: "+((t_view  -t_make )       )+"ms"+"\n"+
    //                 "Remv: "+((t_remove-t_view )*1.0/NUMKEYS)+"\n"
    //                 );
  }

  // Bulk inject keys on the local node without any network traffic
  private static class DoKeys extends DRemoteTask<DoKeys> {
    private final boolean _insert;
    private final int _sysnkeys, _usernkeys;
    DoKeys( boolean insert, int sysnkeys, int usernkeys ) { _insert=insert; _sysnkeys = sysnkeys; _usernkeys = usernkeys;}
    @Override public void lcompute() {
      long l=0;
      for( int i=0; i<_sysnkeys+_usernkeys; i++ ) {
        byte[] kb = new byte[2+4+8];
        kb[0] = i<_sysnkeys ? Key.BUILT_IN_KEY : (byte)'_'; // System Key vs User Key
        kb[1] = 0;                                          // No replicas
        kb[2] = 'A';  kb[3] = 'B';  kb[4] = 'C';  kb[5] = 'D';
        while( true ) {
          UDP.set8(kb,6,l++);
          Key k = Key.make(kb);
          if( k.home() ) {
            if( _insert ) DKV.put(k,new Value(k,kb),_fs);
            else          DKV.remove(k,_fs);
            break; 
          }
        }
      }
      tryComplete();
    }
    @Override public void reduce( DoKeys ignore ) { }
  }

}
