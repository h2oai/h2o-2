package water;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.concurrent.ExecutionException;
import jsr166y.CountedCompleter;
import org.junit.BeforeClass;
import org.junit.Test;
import water.fvec.Chunk;
import water.fvec.NFSFileVec;
import water.util.Utils;

public class MRThrow extends TestUtil {

  @BeforeClass public static void stall() { stall_till_cloudsize(3); }

  // ---
  // Map in h2o.jar - a multi-megabyte file - into Arraylets.
  // Run a distributed byte histogram.  Throw an exception in *some* map call,
  // and make sure it's forwarded to the invoke.
  @Test
  public void testInvokeThrow() {
    File file = find_test_file("target/h2o.jar");
    Key h2okey = load_test_file(file);
    NFSFileVec nfs = DKV.get(h2okey).get();
    try {
      for(int i = 0; i < H2O.CLOUD._memary.length; ++i){
        ByteHistoThrow bh = new ByteHistoThrow();
        bh._throwAt = H2O.CLOUD._memary[i].toString();
        try {
          bh.doAll(nfs); // invoke should throw DistributedException wrapped up in RunTimeException
          fail("should've thrown");
        } catch(RuntimeException e){
          assertTrue(e.getMessage().contains("test"));
        } catch(Throwable ex){
          ex.printStackTrace();
          fail("Expected RuntimeException, got " + ex.toString());
        }
      }
    } finally {
      // currently canceled RPC calls do not properly wait for all other nodes...
      // so once a map() call fails, other map calls can lazily load data after we call delete()
      try { Thread.sleep(100); } catch( InterruptedException ignore ) {}
      Lockable.delete(h2okey);
    }
  }

  @Test 
  public void testGetThrow() {
    File file = find_test_file("target/h2o.jar");
    Key h2okey = load_test_file(file);
    NFSFileVec nfs = DKV.get(h2okey).get();
    try {
      for(int i = 0; i < H2O.CLOUD._memary.length; ++i){
        ByteHistoThrow bh = new ByteHistoThrow();
        bh._throwAt = H2O.CLOUD._memary[i].toString();
        try {
          bh.dfork(nfs).get(); // invoke should throw DistributedException wrapped up in RunTimeException
          fail("should've thrown");
        } catch(ExecutionException e){
          assertTrue(e.getMessage().contains("test"));
        } catch(Throwable ex){
          ex.printStackTrace();
          fail("Expected ExecutionException, got " + ex.toString());
        }
      }
    } finally {
      // currently canceled RPC calls do not properly wait for all other nodes...
      // so once a map() call fails, other map calls can lazily load data after we call delete()
      try { Thread.sleep(100); } catch( InterruptedException ignore ) {}
      Lockable.delete(h2okey);
    }
  }

  @Test
  public void testContinuationThrow() throws InterruptedException, ExecutionException {
    File file = find_test_file("target/h2o.jar");
    Key h2okey = load_test_file(file);
    NFSFileVec nfs = DKV.get(h2okey).get();
    try {
      for(int i = 0; i < H2O.CLOUD._memary.length; ++i){
        ByteHistoThrow bh = new ByteHistoThrow();
        bh._throwAt = H2O.CLOUD._memary[i].toString();
        final boolean [] ok = new boolean[]{false};
        try {
          bh.setCompleter(new CountedCompleter() {
            @Override public void compute() {}
            @Override public boolean onExceptionalCompletion(Throwable ex, CountedCompleter cc){
              ok[0] = ex.getMessage().contains("test");
              return true;
            }
          });
          bh.dfork(nfs).get(); // invoke should throw DistrDTibutedException wrapped up in RunTimeException
          assertTrue(ok[0]);
        }catch(ExecutionException eex){
          assertTrue(eex.getCause().getMessage().contains("test"));
        } catch(Throwable ex){
          ex.printStackTrace();
          fail("Unexpected exception" + ex.toString());
        }
      }
    } finally {
      // currently canceled RPC calls do not properly wait for all other nodes...
      // so once a map() call fails, other map calls can lazily load data after we call delete()
      try { Thread.sleep(100); } catch( InterruptedException ignore ) {}
      Lockable.delete(h2okey);
    }
  }

  // Byte-wise histogram
  public static class ByteHistoThrow extends MRTask2<ByteHistoThrow> {
    int[] _x;
    String _throwAt;
    // Count occurrences of bytes
    @SuppressWarnings("divzero")
    @Override public void map( Chunk chk ) {
      _x = new int[256];        // One-time set histogram array
      byte[] bits = chk._mem;   // Compute local histogram
      for( byte b : bits )
        _x[b&0xFF]++;
      if(H2O.SELF.toString().equals(_throwAt))
        throw new RuntimeException("test");
    }
    // ADD together all results
    @Override public void reduce( ByteHistoThrow bh ) { Utils.add(_x,bh._x); }
  }
}
