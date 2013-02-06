package test;

import hex.KMeans;
import java.util.Arrays;
import java.util.Random;
import org.junit.*;
import water.*;

public class ConcurrentKeyTest extends TestUtil {

  @BeforeClass public static void stall() { stall_till_cloudsize(1); }

  // Test
  @Test
  public void testParse() {
    for( int i=0; i<100; i++ ) {
      System.out.println(i);
      //Key k1 = loadAndParseKey("h.hex","smalldata/fail2_24_100000_10.csv.gz");
      Key k1 = loadAndParseKey("h.hex","smalldata/fail1_100x11000.csv.gz");
      UKV.remove(k1);
    }
  }
}
