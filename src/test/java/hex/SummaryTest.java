package hex;

import static org.junit.Assert.assertEquals;
import org.junit.BeforeClass;
import org.junit.Test;
import water.*;
import hex.ColSummaryTask;

public class SummaryTest extends TestUtil {

  @BeforeClass public static void stall() { stall_till_cloudsize(1); }

  // ==========================================================================
  @Test public void testConstSummary() {
    Key vkey = loadAndParseFile("con.hex","./smalldata/constantColumn.csv");
    ValueArray ary = UKV.get(vkey);
    int[] cols = new int[ary.numCols()];
    for( int i=0; i<cols.length; i++ ) cols[i]=i;
    Summary sum = new ColSummaryTask(ary,cols).invoke(vkey).result();
    for( int i=0; i<cols.length; i++ ) {
      System.out.println("col "+i);
      sum._sums[i].toJson();
    }

    Summary.ColSummary csum = sum._sums[0];
    assertEquals(1,csum._bins.length);
    assertEquals(ary.length(),csum._bins[0]);
    UKV.remove(vkey);
  }
}
