package water;

import org.junit.BeforeClass;
import org.junit.Test;

public class ConcurrentKeyTest extends TestUtil {

  @BeforeClass public static void stall() { stall_till_cloudsize(2); }

  // Test
  @Test
  public void testParse() {
    for( int i=0; i<25; i++ ) {// One iteration to keep it fast
      //Key k1 = loadAndParseKey("h.hex","smalldata/fail2_24_100000_10.csv.gz");
      Key k1 = load_test_file("smalldata/fail1_100x11000.csv.gz");
      Lockable.delete(k1);
    }
  }
}
