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
    for( int i=0; i<cols.length; i++ ) {
      sum._sums[i].toJson();
    }

    Summary.ColSummary csum = sum._sums[0];
    assertEquals(1,csum._bins.length);
    assertEquals(ary.length(),csum._bins[0]);
    assertEquals(0, csum._n_na);
    assertEquals(0, csum._nzero);
    UKV.remove(vkey);
  }


  public void testNonConstSummary(){
    Key vkey = loadAndParseFile("enum_test.hex","./smalldata/test/test_percentiles_distns.csv.gz");
    try {
      ValueArray array = UKV.get(vkey);
      int[] cols = new int[ 2 ];

      // search for columns zeroone and zerotwo
      String[] colnames = array.colNames();
      for( int i=0; i<cols.length; i++ ){
        if ( colnames[ i ].equals("zeroone") )
          cols[ 0 ] = i;
        else if ( colnames[i].equals( "zerotwo" ) )
          cols[ 1 ] = i;
      }

      Summary sum = new ColSummaryTask(array,cols).invoke(vkey).result();
      for( int i=0; i<cols.length; i++ ) {
        sum._sums[i]._summary = sum;
        sum._sums[i].toJson();
      }

      // column zerooneF
      Summary.ColSummary csum = sum._sums[0];
      assertEquals(2, csum._bins.length);
      assertEquals(0, csum._n_na);
      assertEquals(520, csum._nzero);
      assertEquals(520, csum._bins[ 0 ]);
      assertEquals(480, csum._bins[ 1 ]);

      // column zerotwoF
      csum = sum._sums[1];
      assertEquals(3, csum._bins.length);
      assertEquals(0, csum._n_na);
      assertEquals(334, csum._nzero);
      assertEquals(334, csum._bins[ 0 ]);
      assertEquals(329, csum._bins[ 1 ]);
      assertEquals(337, csum._bins[ 1 ]);
    } finally {
      if (vkey != null)
        UKV.remove( vkey );
    }
  }

  public void testEnumSummary(){
    //test_percentiles_00.csv

    Key vkey = loadAndParseFile("enum_test.hex","./smalldata/test/test_percentiles_distns.csv.gz");
    try {
      ValueArray array = UKV.get(vkey);
      int[] cols = new int[ 2 ];

      // search for columns zerooneF and zerotwoF
      for( int i=0; i<cols.length; i++ ){
        if ( array.colNames()[ i ].equals("zerooneF") )
          cols[ 0 ] = i;
        else if ( array.colNames()[i].equals( "zerotwoF" ) )
          cols[ 1 ] = i;
      }

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
      assertEquals(520, csum._bins[ 0 ]);
      assertEquals(480, csum._bins[ 1 ]);

      // column zerotwoF
      csum = sum._sums[1];
      assertEquals(3, csum._bins.length);
      assertEquals(3, csum.getEnumCardinality());
      assertEquals(0, csum._n_na);
      assertEquals(334, csum._bins[ 0 ]);
      assertEquals(329, csum._bins[ 1 ]);
      assertEquals(337, csum._bins[ 1 ]);
    } finally {
      if (vkey != null)
        UKV.remove( vkey );
    }
  }



}
