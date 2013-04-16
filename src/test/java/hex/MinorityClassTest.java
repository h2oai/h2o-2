package hex;

import static org.junit.Assert.assertTrue;
import hex.rf.MinorityClasses.UnbalancedClass;
import hex.rf.MinorityClasses;
import java.io.FileInputStream;
import java.util.Arrays;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;
import water.*;
import water.parser.ParseDataset;

public class MinorityClassTest extends TestUtil {
  static final int _classIdx = 10;

  @BeforeClass public static void stall() { stall_till_cloudsize(3); }

  static int [] expectedHist = new int [] {501209,422498,47622,21121,3885,1996,1424,230,12,3};

  @Test public void testHistogram(){
    Key key = loadAndParseKey("poker.hex","smalldata/poker/poker-hand-testing.data");
    ValueArray data = DKV.get(key).get();
    int [] h = MinorityClasses.globalHistogram(MinorityClasses.histogram(data, _classIdx));
    assertTrue(Arrays.equals(expectedHist, h));
    UKV.remove(key);
  }

  @Test public void testExtraction(){
    Key key = loadAndParseKey("poker.hex","smalldata/poker/poker-hand-testing.data");
    ValueArray data = DKV.get(key).get();
    UnbalancedClass [] uClasses = MinorityClasses.extractUnbalancedClasses(data, 10, new int [] {0,9});
    final int n = new int[]{6,3,2}[ValueArray.LOG_CHK-20];
    UnbalancedClass[] goal = new UnbalancedClass[] {
      new UnbalancedClass(0,new Key[n],501209), // Class 0, 501209 rows
      new UnbalancedClass(9,new Key[1],     3)  // Class 9,      3 rows
    };
    for( int i=0; i<uClasses.length; i++ ) {
      UnbalancedClass u = uClasses[i];
      UnbalancedClass g = goal    [i];
      assertTrue(u._c == g._c);
      assertTrue(u._rows == g._rows);
      assertTrue(u._chunks.length == g._chunks.length);
      for( Key k : u._chunks )
        UKV.remove(k);
    }
    UKV.remove(key);
  }

  public static void main(String [] args){
    if(args.length > 0){
      assert args.length == 1:"unexpected number of args, expects exactly one arg (number of nodes), got " + args.length;
    }
    JUnitCore junit = new JUnitCore();
    Result r = junit.run(MinorityClassTest.class);
    System.out.println("======================================");
    if(r.wasSuccessful()){
      System.out.println("All tests finished successfully!");
    } else {
      System.out.println("Finished with failures:");
      for(Failure f:r.getFailures()){
        System.err.println(f.toString());
        System.err.println(f.getTrace());
      }
    }
    System.exit(0);
  }
}
