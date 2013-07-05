package water;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import java.io.File;
import org.junit.*;

public class MRThrow extends TestUtil {

  @BeforeClass public static void stall() { stall_till_cloudsize(3); }

  // ---
  // Map in h2o.jar - a multi-megabyte file - into Arraylets.  
  // Run a distributed byte histogram.  Throw an exception in *some* map call,
  // and make sure it's forwarded to the invoke.
  @Test public void testMRThrow() {
    File file = find_test_file("target/h2o.jar");
    Key h2okey = load_test_file(file);
    ByteHistoThrow bh = new ByteHistoThrow();
    try { 
      bh.invoke(h2okey);
    } catch( DException.DistributedException de ) {
      assertTrue(de.getMessage().indexOf("/ by zero") != -1);
      //de.printStackTrace();
      return;
    } catch( RuntimeException de ) {
      de.printStackTrace();
      fail("Should have thrown a DistributedException");
    } finally {
      UKV.remove(h2okey);
    }
    fail("Should have thrown from the invoke");
  }
  
  // Byte-wise histogram
  public static class ByteHistoThrow extends MRTask<ByteHistoThrow> {
    int[] _x;
    // Count occurrences of bytes
    @SuppressWarnings("divzero")
    public void map( Key key ) {
      _x = new int[256];        // One-time set histogram array
      Value val = DKV.get(key); // Get the Value for the Key
      byte[] bits = val.memOrLoad();  // Compute local histogram
      for( int i=0; i<bits.length; i++ )
        _x[bits[i]&0xFF]++;
      if( ValueArray.getChunkIndex(key)==6 )
        _x[0] = 1/0;            // Throw an exception on a random chunk
    }
    // ADD together all results
    public void reduce( ByteHistoThrow bh ) {
      if( _x == null ) { _x = bh._x; return; }
      for( int i=0; i<_x.length; i++ )
        _x[i] += bh._x[i];
    }
  }
}
