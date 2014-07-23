package hex;

import static org.junit.Assert.assertEquals;

import org.junit.BeforeClass;
import org.junit.Test;

import water.*;

public class SummaryTest extends TestUtil {

  @BeforeClass public static void stall() { stall_till_cloudsize(1); }

  // ==========================================================================
  @Test public void testConstSummary() {
    Key vkey = loadAndParseFile("con.hex","./smalldata/constantColumn.csv");
//    Key vkey = loadAndParseFile("enum_test.hex","./smalldata/test/test_percentiles_distns.csv");
    ValueArray ary = UKV.get(vkey);
    int[] cols = new int[ary.numCols()];
    for( int i=0; i<cols.length; i++ ) cols[i]=i;
    Summary sum = new ColSummaryTask(ary,cols).invoke(vkey).result();
    sum.toJson();

    Summary.ColSummary csum = sum._sums[0];
    assertEquals(1,csum._bins.length);
    assertEquals(ary.length(),csum._bins[0]);
    assertEquals(0, csum._n_na);
    assertEquals(0, csum._nzero);
    ary.delete();
  }


  @Test public void testNonConstSummary(){
    Key vkey = loadAndParseFile("enum_test.hex","./smalldata/test/test_percentiles_distns.csv.gz");
    ValueArray array = UKV.get(vkey);
    try {
      int[] cols = new int[ 2 ];

      // search for columns zeroone and zerotwo
      String[] colnames = array.colNames();
      int found_count = 0;
      for( int i=0; i<colnames.length; i++ ){
        if ( colnames[ i ].equals("zeroone") ){
          cols[ 0 ] = i;
          found_count++;
        } else if ( colnames[i].equals( "zerotwo" ) ){
          cols[ 1 ] = i;
          found_count++;
        }
      }
      assertEquals("found columns zeroone and zerotwo", 2, found_count);

      Summary sum = new ColSummaryTask(array,cols).invoke(vkey).result();
      for( int i=0; i<cols.length; i++ ) {
        sum._sums[i]._summary = sum;
        sum._sums[i].toJson();
      }

      // column zerooneF
      Summary.ColSummary csum = sum._sums[0];
      assertEquals(2, csum._bins.length);
      assertEquals(0, csum._n_na);
      assertEquals(497, csum._nzero);
      assertEquals(497, csum._bins[ 0 ]);
      assertEquals(503, csum._bins[ 1 ]);

      // column zerotwoF
      csum = sum._sums[1];
      assertEquals(3, csum._bins.length);
      assertEquals(0, csum._n_na);
      assertEquals(353, csum._nzero);
      assertEquals(353, csum._bins[ 0 ]);
      assertEquals(316, csum._bins[ 1 ]);
      assertEquals(331, csum._bins[ 2 ]);
    } finally {
      array.delete();
    }
  }

  @Test public void testEnumSummary(){
    Key vkey = loadAndParseFile("enum_test.hex","./smalldata/test/test_percentiles_distns.csv.gz");
    ValueArray array = UKV.get(vkey);
    try {
      int[] cols = new int[ 2 ];

      // search for columns zerooneF and zerotwoF

      // search for columns zeroone and zerotwo
      String[] colnames = array.colNames();
      int found_count = 0;
      for( int i=0; i<colnames.length; i++ ){
        if ( colnames[ i ].equals("zerooneF") ){
          cols[ 0 ] = i;
          found_count++;
        } else if ( colnames[i].equals( "zerotwoF" ) ){
          cols[ 1 ] = i;
          found_count++;
        }
      }
      assertEquals("found columns zerooneF and zerotwoF", 2, found_count);

      Summary sum = new ColSummaryTask(array,cols).invoke(vkey).result();
      for( int i=0; i<cols.length; i++ ) {
        sum._sums[i]._summary = sum;
        sum._sums[i].toJson();
      }

      // column zerooneF
      Summary.ColSummary csum = sum._sums[0];
      assertEquals(2, csum._bins.length);
      assertEquals(2, csum.getEnumCardinality());
      assertEquals(0, csum._n_na);
      assertEquals(497, csum._bins[ 0 ]);
      assertEquals(503, csum._bins[ 1 ]);

      // column zerotwoF
      csum = sum._sums[1];
      assertEquals(3, csum._bins.length);
      assertEquals(3, csum.getEnumCardinality());
      assertEquals(0, csum._n_na);
      assertEquals(353, csum._bins[ 0 ]);
      assertEquals(316, csum._bins[ 1 ]);
      assertEquals(331, csum._bins[ 2 ]);
    } finally {
      array.delete();
    }
  }



}
